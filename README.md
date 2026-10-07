# 本地数据管道编排与血缘审计服务

用本地 JSON 存储模拟管道定义、运行记录与血缘关系，提供依赖编排执行、
失败传播、并发/幂等控制、取消、历史版本复现与**定点续跑**能力。所有入口
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
  └─ StepRun(step_name, status, outputs, error, reused, ...)
LineageRecord(run_id, step_name, inputs, outputs, step_config,
              pipeline_version, reused_from_run_id)
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
       PENDING → SUCCEEDED                              （仅续跑沿用旧结果，不执行）
       PENDING → SKIPPED_UPSTREAM_FAILED   （直接上游 FAILED，被直接拖累）
       PENDING → SKIPPED_TRANSITIVE         （直接上游没失败但也没成功，更上游间接连累）
       PENDING → SKIPPED_CANCELLED         （运行取消，拒绝执行）
```

三种拒绝原因可区分：**直接上游失败**（`SKIPPED_UPSTREAM_FAILED`，直接依赖里
存在 FAILED）、**更上游间接连累**（`SKIPPED_TRANSITIVE`，直接依赖自身也是被
跳过的、链路更上游才是失败点）、**取消**（`SKIPPED_CANCELLED`）。失败传播
与取消不会混淆。终态不可逆，表外的流转一律抛 `InvalidTransitionError`，
不会被静默覆盖。

## 定点续跑

一条管道中途某步失败后，修好原因只想重跑失败步骤及被它连累、当时没跑的部分，
前面已成功的步骤直接沿用旧结果。续跑入口：

```python
run = service.resume(source_run_id, idempotency_key)   # 直接调用
# HTTP：POST /runs/{source_run_id}/resume  body: {"idempotency_key": "..."}
```

续跑语义：

- **全新运行、同等约束**：续跑生成新的 `run_id`，通过 `resumed_from_run_id`
  交代从哪次运行接过来。它照样受"同一管道同一时刻只有一个活动运行"、
  "幂等键重复提交返回既有运行"、"运行可取消"三条规则约束；
  并发的异键续跑会有一个被 `ConflictError`（HTTP 409）拒绝，
  同键并发/重复续跑返回同一运行，处理器不会执行第二次。
- **只重跑该重跑的**：源运行当时状态为 `SUCCEEDED` 的步骤标记
  `reused=true`，输出原样交给后续步骤，**不调用 handler、无重复副作用**；
  其余步骤（FAILED / 当时被跳过 / 无记录）回到 PENDING 按依赖序重跑。
  续跑中再失败，其下游依旧不执行，且沿用上面的三类原因区分。
- **按当时的版本，不挑最新**：续跑深拷贝源运行内嵌的
  `definition_snapshot`，`pipeline_version` 也取源运行的值。中间改过配置、
  改过步骤名、做过版本回滚，都不影响续跑——它和源运行可对着看。
- **源运行只读不改**：源运行的状态、步骤结论与血缘记录都不被修改或覆盖；
  沿用步骤的血缘作为**新记录只追加到新运行**名下，并通过
  `reused_from_run_id` 指向源运行。`reproduce()` 的输出里，运行级有
  `resumed_from_run_id`，每个步骤有 `reused`、血缘里有
  `reused_from_run_id`，一眼看出哪些结果是沿用、哪些是本次重算。
- **取消一致性**：续跑期间取消同样在步骤边界生效——正在执行的步骤作为
  原子单位允许完成并只生效一次，已沿用步骤保留，未开始的步骤统一
  `SKIPPED_CANCELLED`，运行终态为 `CANCELLED`，不留下半生效中间结果。

明确拒绝（`ResumeNotPossibleError`，HTTP 409）而不是返回空运行：

| 源运行情况 | 结果 |
| --- | --- |
| 不存在 | `NotFoundError`（404） |
| 尚未结束（PENDING/RUNNING） | 409，源运行未结束 |
| 已全部成功（SUCCEEDED） | 409，无需续跑 |
| 已结束但没有未成功步骤 | 409，没有可续跑步骤 |
| 同管道另有活动运行 | `ConflictError`（409） |

## 一致性规则

1. **依赖推进与失败传播**：按拓扑序执行；直接上游里存在 FAILED 的步骤标记
   `SKIPPED_UPSTREAM_FAILED`，直接上游自身也被跳过的更下游步骤标记
   `SKIPPED_TRANSITIVE`（间接连累），取消导致的标记 `SKIPPED_CANCELLED`，
   三类拒绝原因互不混淆；失败步骤零输出、零血缘。任一步骤失败则运行整体
   `FAILED`。
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
   续跑沿用的成功步骤会在新运行名下追加一条标注 `reused_from_run_id`
   的血缘记录，源运行血缘不变。`reproduce(run_id)` 完全基于运行快照与
   血缘记录重建结论，与当前定义无关，并通过 `resumed_from_run_id` /
   `reused` 交代续跑链与沿用关系。
7. **兼容范围**：模型新增字段（`Run.resumed_from_run_id`、
   `StepRun.reused`、`LineageRecord.reused_from_run_id`）反序列化时
   均带默认值，旧版 JSON 存储可直接读取；`SKIPPED_TRANSITIVE` 是新增的
   步骤终态，历史运行中不会出现该状态。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/pipelines` | 注册管道（v1），重复注册 409 |
| PUT | `/pipelines/{name}?note=` | 提交定义变更，产生新版本 |
| POST | `/pipelines/{name}/rollback/{version}` | 回滚：以历史定义创建新版本 |
| GET | `/pipelines/{name}/versions` | 版本列表 |
| POST | `/pipelines/{name}/runs` | 触发运行（body: `idempotency_key`） |
| POST | `/runs/{run_id}/resume` | 定点续跑（body: `idempotency_key`），非法续跑 409 |
| POST | `/runs/{run_id}/cancel` | 取消活动运行，终态运行 409 |
| GET | `/runs/{run_id}` | 查询运行 |
| GET | `/runs/{run_id}/lineage` | 血缘记录 |
| GET | `/runs/{run_id}/reproduce` | 复现历史运行结论 |

错误映射：`NotFoundError→404`，`ConflictError/InvalidTransitionError/
ResumeNotPossibleError→409`，`ValidationError→422`。

## 验证方法

`uv run pytest` 运行 42 个用例，日志末尾按业务域汇总覆盖情况：

- **依赖失败传播**：上游失败时直接下游被直接拖累、更下游为间接连累，
  两类跳过与取消三者可区分，失败步骤零副作用零血缘。
- **并发与重复提交**：8 线程同键并发触发只执行一次；活动运行期间
  异键触发被 409 拒绝；同键重复提交返回同一运行。
- **版本与血缘**：配置变更、步骤改名后历史运行复现结论不变；
  回滚产生新版本且历史版本与历史运行均可追溯。
- **取消一致性**：运行中取消后停在一致状态、未开始步骤零副作用、
  取消后可立即再次触发。
- **非法流转/非法定义**：终态取消、重复注册、幽灵更新、依赖环、
  未知依赖、步骤重名、自依赖均被明确拒绝。
- **定点续跑**：成功步骤沿用旧结果不重算、旧输出喂给下游、源运行
  只读不改；配置变更/改名/回滚后续跑仍按源运行当时的版本与步骤名。
- **续跑失败传播**：续跑中再次失败时直接下游 `SKIPPED_UPSTREAM_FAILED`、
  更下游 `SKIPPED_TRANSITIVE`，与取消跳过三类可区分。
- **非法续跑拒绝**：源运行不存在 404，未结束/全成功/被活动运行占用
  均 409 明确拒绝，不返回空运行。
- **并发/重复续跑**：8 线程同键并发续跑只产生一个运行、修复步骤只执行
  一次；异键并发续跑其一被 409 拒绝；同键重复续跑返回同一运行。
- **续跑取消**：取消停在一致状态、沿用步骤保留、正在执行步骤原子完成、
  未开始步骤为取消跳过。
- **跨运行追溯**：沿用血缘挂在新运行名下并标注来源；`reproduce()`
  交代 `resumed_from_run_id` 与每步 `reused`；源运行复现结论不变。
- **续跑接口**：HTTP 发起续跑与直接调用 `service.resume` 结论一致，
  对全成功运行续跑返回 409 而非空运行。

## 目录结构

```
pipeline/
  errors.py     # 业务错误类型（可预期拒绝 vs 系统故障）
  models.py     # 领域模型 + JSON 编解码
  store.py      # 本地 JSON 存储（RLock + 原子写盘）
  registry.py   # 定义校验、不可变版本、回滚
  executor.py   # 编排执行：拓扑推进/失败传播/取消/幂等/单活动运行
  lineage.py    # 血缘记录与历史复现
  service.py    # 门面：所有入口共享同一实例
main.py         # FastAPI HTTP 入口
tests/          # 按业务域组织的单测 + 覆盖汇总日志
```
