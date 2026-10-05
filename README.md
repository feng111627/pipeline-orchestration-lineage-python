# 数据管道编排与血缘审计服务

运行在本地的编排服务：用本地 JSON 存储模拟管道定义、运行记录与血缘关系，
提供 HTTP 入口（FastAPI）与可直接调用的 Python API。所有入口共享同一份
`Store / Registry / Executor / LineageService`，因此编排执行、失败传播与
历史复现在任何入口下结论一致。

## 编排模型

- **管道定义（PipelineVersion）**：一组带依赖关系的步骤（`StepDef`），
  每次变更生成一个**单调递增、不可变**的版本。注册期校验：步骤名唯一、
  依赖必须存在、不允许依赖环。
- **运行（RunRecord）**：触发时锚定当时的最新版本号与输入快照，
  状态机为 `PENDING -> RUNNING -> {SUCCEEDED, FAILED, CANCELLED}`。
- **步骤运行（StepRunRecord）**：状态机为
  `PENDING -> RUNNING -> {SUCCEEDED, FAILED}`，或 `PENDING -> {SKIPPED, CANCELLED}`。
  执行器按拓扑序推进，仅当全部上游 `SUCCEEDED` 才调度下游。
- **血缘（LineageRecord）**：每个成功步骤记录运行输入、上游输出、自身输出
  及其锚定的管道版本，追加式写入，永不修改。

## 一致性规则

1. **失败传播**：步骤失败后，直接下游标记 `SKIPPED/UPSTREAM_FAILED`，
   间接下游标记 `SKIPPED/UPSTREAM_SKIPPED`，原因可区分；被跳过步骤的
   处理器绝不执行，无任何副作用。
2. **原子生效**：步骤输出只在成功时一次性写入；失败步骤不留半截输出。
   取消只阻止后续调度，正在执行的步骤走到成功/失败终态，未开始步骤标记
   `CANCELLED`，运行停在一致的 `CANCELLED` 终态。
3. **并发与幂等**：同一管道同一时刻只允许一个活动运行（触发时持锁做
   "检查-创建"），并发触发得到 `409 ConflictError`；相同幂等键重复提交
   返回同一运行，不产生重复执行或重复副作用。
4. **非法流转拒绝**：对终态运行取消、回滚到不存在的版本、注册非法定义等，
   均抛出明确异常（HTTP 409/404/422），绝不静默覆盖状态。
5. **历史可复现**：版本不可变，回滚是"用旧内容生成新版本"，不修改历史；
   因此配置变更、步骤改名、回滚之后，任何已完成运行都能按其锚定版本
   复现出相同结论（`GET /runs/{id}/replay`，只读无副作用）。

## 目录结构

```
pipeline/
  models.py    # 状态机与数据模型（运行/步骤/版本/血缘）
  errors.py    # 业务异常：NotFound/Conflict/InvalidState/Validation
  store.py     # 本地 JSON 存储：原子写 + 进程内锁
  registry.py  # 定义校验、版本化注册、回滚
  executor.py  # 依赖推进、失败传播、取消、并发与幂等控制
  lineage.py   # 血缘查询与历史复现
main.py        # FastAPI HTTP 入口
tests/         # 按业务域划分的测试
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/pipelines/{id}/versions` | 注册新版本（校验依赖与环） |
| POST | `/pipelines/{id}/rollback` | 回滚：以历史版本内容生成新版本 |
| GET  | `/pipelines/{id}/versions/{v}` | 查询指定版本定义 |
| POST | `/pipelines/{id}/runs` | 触发运行（支持 `idempotency_key`） |
| GET  | `/runs/{run_id}` | 查询运行与步骤状态 |
| POST | `/runs/{run_id}/cancel` | 取消运行（终态则 409） |
| GET  | `/runs/{run_id}/lineage` | 查询运行血缘 |
| GET  | `/runs/{run_id}/replay` | 按锚定版本复现运行结论 |

启动：`uv run python main.py`（数据目录由 `PIPELINE_DATA_DIR` 指定，默认 `./data`）。

## 验证方法

```bash
uv run pytest          # -v -s 输出中标注每个用例的业务域与验证点
```

测试按业务域组织，日志中可直接看到覆盖情况：

- `test_failure_propagation.py`（依赖失败传播）：跳过原因可区分、
  失败步骤无半截输出、被跳过步骤处理器不执行。
- `test_concurrency.py`（并发与重复提交）：并发触发仅一个活动运行、
  幂等键去重无副作用、终态后非法流转被拒绝。
- `test_cancellation.py`（取消一致性）：执行中取消停在一致终态、
  未开始步骤不执行、取消后可再次触发。
- `test_versioning.py`（版本与复现）：版本不可变且单调、回滚不改历史、
  配置变更/改名/回滚后历史运行仍可复现。
- `test_lineage.py`（血缘审计）：输入/输出/配置版本可追溯、
  未执行步骤无血缘、血缘锚定执行时版本。
