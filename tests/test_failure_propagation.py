"""业务域：依赖失败传播 —— 上游失败时下游不得执行，拒绝原因可区分。"""
from __future__ import annotations

from pipeline.models import RunStatus, SkipReason, StepDef, StepStatus


def _diamond(registry, fail_at: str | None = None):
    """a -> {b, c} -> d 的菱形依赖；fail_at 指定失败步骤。"""
    def handler(name):
        def run(config, inputs, upstream):
            if name == fail_at:
                raise RuntimeError(f"{name} exploded")
            return {"by": name}
        return run
    steps = [StepDef("a"), StepDef("b", ("a",)), StepDef("c", ("a",)), StepDef("d", ("b", "c"))]
    registry.create_version("pipe", steps)
    return {s.name: handler(s.name) for s in steps}


def test_upstream_failure_skips_downstream_with_distinguishable_reasons(registry, executor):
    """上游失败时，直接下游标记 UPSTREAM_FAILED，间接下游标记 UPSTREAM_SKIPPED。"""
    handlers = _diamond(registry, fail_at="b")
    for name, h in handlers.items():
        executor.register_handler(name if False else "noop", lambda c, i, u: {})
    # 用 handler 名区分步骤行为
    registry.create_version("pipe2", [
        StepDef("a", handler="ok"), StepDef("b", ("a",), handler="boom"),
        StepDef("c", ("a",), handler="ok"), StepDef("d", ("b", "c"), handler="ok"),
    ])
    executor.register_handler("ok", lambda c, i, u: {"done": True})
    def boom(c, i, u):
        raise RuntimeError("b exploded")
    executor.register_handler("boom", boom)

    run = executor.trigger("pipe2")

    assert run.status == RunStatus.FAILED
    assert run.step_runs["a"].status == StepStatus.SUCCEEDED
    assert run.step_runs["b"].status == StepStatus.FAILED
    assert "b exploded" in run.step_runs["b"].error
    # 与失败无关的分支照常执行
    assert run.step_runs["c"].status == StepStatus.SUCCEEDED
    # 直接依赖失败步骤 -> UPSTREAM_FAILED 不可区分地混为 UPSTREAM_SKIPPED 是缺陷
    assert run.step_runs["d"].status == StepStatus.SKIPPED
    assert run.step_runs["d"].skip_reason == SkipReason.UPSTREAM_FAILED


def test_transitive_skip_reason_is_upstream_skipped(registry, executor):
    """链式依赖 a->b->c->d：b 失败时 c 为 UPSTREAM_FAILED，d 为 UPSTREAM_SKIPPED。"""
    registry.create_version("chain", [
        StepDef("a", handler="ok"), StepDef("b", ("a",), handler="boom"),
        StepDef("c", ("b",), handler="ok"), StepDef("d", ("c",), handler="ok"),
    ])
    executor.register_handler("ok", lambda c, i, u: {})
    def boom(c, i, u):
        raise ValueError("bad")
    executor.register_handler("boom", boom)

    run = executor.trigger("chain")

    assert run.status == RunStatus.FAILED
    assert run.step_runs["c"].status == StepStatus.SKIPPED
    assert run.step_runs["c"].skip_reason == SkipReason.UPSTREAM_FAILED
    assert run.step_runs["d"].status == StepStatus.SKIPPED
    assert run.step_runs["d"].skip_reason == SkipReason.UPSTREAM_SKIPPED


def test_failed_step_has_no_partial_outputs(registry, executor):
    """失败步骤不得留下半截输出：outputs 必须为空，效果原子生效。"""
    def flaky(config, inputs, upstream):
        config["leaked"] = "partial"  # 尝试在失败前产生副作用
        raise RuntimeError("midway")
    registry.create_version("atomic", [StepDef("x", handler="flaky")])
    executor.register_handler("flaky", flaky)

    run = executor.trigger("atomic")

    assert run.step_runs["x"].status == StepStatus.FAILED
    assert run.step_runs["x"].outputs == {}


def test_skipped_step_handler_never_invoked(registry, executor):
    """被拒绝执行的下游步骤，其处理器绝不被调用（无副作用）。"""
    calls = []
    registry.create_version("noside", [
        StepDef("a", handler="boom"), StepDef("b", ("a",), handler="spy"),
    ])
    def boom(c, i, u):
        raise RuntimeError("x")
    executor.register_handler("boom", boom)
    executor.register_handler("spy", lambda c, i, u: calls.append("b") or {})

    executor.trigger("noside")

    assert calls == []
