# 本地数据管道编排与血缘审计服务

用本地 JSON 存储模拟管道定义、运行记录与血缘关系，提供依赖编排执行、
失败传播、并发/幂等控制、取消与历史版本复现能力。所有入口
（HTTP API、单元测试、脚本）共享同一个 `PipelineService` 门面，
因此在任何入口下一致性结论相同。

## 运行方式

```bash
uv sync                 # 安装依赖
uv run python main.py   # 启动 HTTP 服务（PIPELINE_DB 环境变量指定存储文件，默认 pipeline_db.json）
uv run pytest           # 运行全部单测，日志末尾输出业务覆盖汇总
```

## 编排模型

```
PipelineDefinition(name, steps[])
  └─ StepDefinition(name, handler, depends_on[], config{})
PipelineVersion(pipeline_name, version, definition, created_at, note)   # 不可变
Run(run_id, pipeline_name, pipeline_version, idempotency_key,
    definition_snapshot, status, step_runs{}, resumed_from_run_id)      # 内嵌定义快照
  └─ StepRun(step_name, status, outputs, error, origin_run_id, ...)
LineageRecord(run_id, step_name, inputs, outputs, step_config,
              pipeline_version, carried_from_run_id)
```

- **定义即 DAG**：步骤通过 `depends_on` 声明依赖，注册时校验
  （非空、步骤名唯一、依赖存在、无环），非法定义被 `ValidationError` 拒绝，
  不进入版本历史。
- **版本不可变**：`register` 产生 v1，`update` 与 `rollback` 都追加新版本；
  历史版本永不修改。回滚 = 用历史定义创建一个新版本（note 记录来源），
  因此回滚不破坏已完成运行的可追溯性。
- **运行冻结快照**：触发时把当前版本定义完整拷贝进 `Run.definition_snapshot`，
  执行与复现都只读快照，之后的配置变更、步骤改名不影响历史结论。
- **处理器契约**：`handler(config, upstream_outputs) -> outputs`。
  handler 只做计算，副作用（输出、血缘）由编排器在步骤成功后统一生效。

### 状态机

```
运行:  PENDING → RUNNING → SUCCEEDED / FAILED / CANCELLED
步骤:  PENDING → RUNNING → SUCCEEDED / FAILED
       PENDING → SKIPPED_UPSTREAM_FAILED   （直接上游失败，拒绝执行）
       PENDING → SKIPPED_UPSTREAM_SKIPPED  （直接上游本身被拒，更上游连累）
       PENDING → SKIPPED_CANCELLED         （运行取消，拒绝执行）
```

三种拒绝原因可区分：被直接失败的上游拖累、被更上游连累、运行取消，
不会混淆。终态不可逆，表外的流转一律抛 `InvalidTransitionError`，
不会被静默覆盖。

## 一致性规则

1. **依赖推进与失败传播**：按拓扑序执行；任一直接上游未成功，
   该步骤标记 `SKIPPED_UPSTREAM_FAILED` 并记录具体上游状态；
   失败步骤零输出、零血缘。任一步骤失败则运行整体 `FAILED`。
2. **原子生效**：步骤的输出、状态与血缘记录在同一把存储锁内一次性落盘；
   handler 抛异常时不产生任何已生效的中间态。存储写盘采用
   "临时文件 + rename"，避免半写文件。
3. **单活动运行**：同一管道同一时刻只允许一个 `PENDING/RUNNING` 运行，
   并发触发抛 `ConflictError`。"检查-创建"在存储锁内完成，无竞态窗口。
4. **幂等触发**：`(pipeline_name, idempotency_key)` 唯一标识一次运行；
   重复提交（含并发）直接返回既有运行，处理器不会执行第二次，
   不产生重复副作用。
5. **取消一致性**：取消在步骤边界生效——正在执行的步骤允许完成
   （步骤是原子单位），未开始的步骤统一标记 `SKIPPED_CANCELLED`，
   运行终态为 `CANCELLED`；取消后管道立即可再次触发。
   对已终态的运行取消会被 `InvalidTransitionError` 拒绝。
6. **血缘可追溯**：每个成功步骤写一条 `LineageRecord`
   （输入=上游输出快照、输出、步骤配置、配置版本号）；
   `reproduce(run_id)` 完全基于运行快照与血缘记录重建结论，
   与当前定义无关。
7. **定点续跑**：见下节。

## 定点续跑（resume）

一条管道跑到中间挂了，修好原因后不必整条重跑：基于一次**已结束
（终态）且存在未成功步骤**的历史运行发起续跑——

```python
resumed = service.resume(source_run_id, idempotency_key="fix-1")
# 或 HTTP：POST /runs/{run_id}/resume  {"idempotency_key": "fix-1"}
```

- **沿用旧结果**：源运行中已 `SUCCEEDED` 的步骤不再执行，其输出直接
  作为下游输入；这些步骤在新运行里仍是 `SUCCEEDED`，但
  `StepRun.origin_run_id` 指向源运行，血缘记录以
  `carried_from_run_id` 标注誊自哪次运行——两次运行之间哪些结果
  是沿用的、哪些是重算的，一眼可辨（`reproduce` 输出同样包含这两个
  字段）。
- **只重跑失败部分**：源运行中 `FAILED` / 各类 `SKIPPED_*` 的步骤
  重置为 `PENDING` 重新执行；再次失败仍按失败传播规则拒绝下游，
  三种拒绝原因（直接上游失败 / 更上游连累 / 取消）保持可区分。
- **版本锚定**：续跑严格使用**源运行当时**的版本号与定义快照
  （深拷贝自源运行，不取当前最新版本），因此中途的配置变更、
  步骤改名、版本回滚都不影响续跑语义；续跑运行与源运行同版本、
  同快照，可直接对照。源运行的记录与血缘**不被修改或覆盖**。
- **全新运行**：续跑产生新的 `run_id`，`resumed_from_run_id` 记录
  来源；它同样受单活动运行、幂等键、取消规则约束——同键重复/并发
  续跑返回同一运行且只执行一次，异键并发续跑被 `ConflictError`
  拒绝，续跑期间取消停在一致状态（沿用步骤保留、未开始步骤
  `SKIPPED_CANCELLED`），且被取消的续跑本身可以再次续跑。
- **明确拒绝**：对全部步骤已成功的运行、不存在的运行、尚未结束的
  运行发起续跑，分别抛 `ValidationError`(422) / `NotFoundError`(404)
  / `ConflictError`(409)，不会返回"什么都没干"的空运行。

**兼容范围**：续跑按源运行快照执行，因此源运行之后发生的任何定义
变更（含回滚）都不影响续跑；唯一的跨版本约束是步骤的 `handler`
注册键仍需可解析（handler 注册表不按版本保存）。旧格式存储记录
（无 `origin_run_id` 等字段）读取时按 `None` 兼容。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/pipelines` | 注册管道（v1），重复注册 409 |
| PUT | `/pipelines/{name}?note=` | 提交定义变更，产生新版本 |
| POST | `/pipelines/{name}/rollback/{version}` | 回滚：以历史定义创建新版本 |
| GET | `/pipelines/{name}/versions` | 版本列表 |
| POST | `/pipelines/{name}/runs` | 触发运行（body: `idempotency_key`） |
| POST | `/runs/{run_id}/resume` | 定点续跑（body: `idempotency_key`），按源运行版本快照 |
| POST | `/runs/{run_id}/cancel` | 取消活动运行，终态运行 409 |
| GET | `/runs/{run_id}` | 查询运行 |
| GET | `/runs/{run_id}/lineage` | 血缘记录 |
| GET | `/runs/{run_id}/reproduce` | 复现历史运行结论 |

错误映射：`NotFoundError→404`，`ConflictError/InvalidTransitionError→409`，
`ValidationError→422`。

## 验证方法

`uv run pytest` 运行 38 个用例，日志末尾按业务域汇总覆盖情况：

- **依赖失败传播**：上游失败时下游被拒绝执行；直接上游失败
  （`SKIPPED_UPSTREAM_FAILED`）、更上游连累（`SKIPPED_UPSTREAM_SKIPPED`）
  与取消三种原因可区分；失败步骤零副作用零血缘。
- **并发与重复提交**：8 线程同键并发触发只执行一次；活动运行期间
  异键触发被 409 拒绝；同键重复提交返回同一运行。
- **版本与血缘**：配置变更、步骤改名后历史运行复现结论不变；
  回滚产生新版本且历史版本与历史运行均可追溯。
- **取消一致性**：运行中取消后停在一致状态、未开始步骤零副作用、
  取消后可立即再次触发。
- **定点续跑**：沿用已成功步骤旧结果（handler 不重算、输出与血缘
  标注来源运行），只重跑失败与受连累步骤；沿用输出直接作为下游
  输入；源运行记录不被修改；被取消的运行也可续跑。
- **续跑版本锚定**：配置变更 + 步骤改名 + 版本回滚之后，续跑仍按
  源运行的版本与快照执行，续跑运行可与源运行对照复现。
- **续跑异常**：续跑再次失败时下游按三分类原因拒绝；续跑期间取消
  停在一致状态，被取消的续跑可再次续跑。
- **非法续跑**：全成功运行（422）、不存在的运行（404）、未结束的
  运行（409）续跑均被明确拒绝。
- **并发与重复续跑**：8 线程同键并发续跑只执行一次；同键重复续跑
  返回同一运行；续跑进行中异键续跑被 409 拒绝。
- **续跑服务接口**：HTTP 入口发起续跑与直接调用结论一致，
  非法续跑在 HTTP 层同样 422。
- **非法流转/非法定义**：终态取消、重复注册、幽灵更新、依赖环、
  未知依赖、步骤重名、自依赖均被明确拒绝。

## 目录结构

```
pipeline/
  errors.py     # 业务错误类型（可预期拒绝 vs 系统故障）
  models.py     # 领域模型 + JSON 编解码
  store.py      # 本地 JSON 存储（RLock + 原子写盘）
  registry.py   # 定义校验、不可变版本、回滚
  executor.py   # 编排执行：拓扑推进/失败传播/取消/幂等/单活动运行/定点续跑
  lineage.py    # 血缘记录、沿用誊写与历史复现
  service.py    # 门面：所有入口共享同一实例
main.py         # FastAPI HTTP 入口
tests/          # 按业务域组织的单测 + 覆盖汇总日志
```
