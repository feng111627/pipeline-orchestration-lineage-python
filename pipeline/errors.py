"""业务异常：所有非法状态流转与冲突都以明确异常拒绝，绝不静默覆盖。"""


class PipelineError(Exception):
    """服务内所有业务错误的基类。"""


class NotFoundError(PipelineError):
    """引用的管道/版本/运行不存在。"""


class ConflictError(PipelineError):
    """并发冲突：如已存在活动运行、幂等键已提交。"""


class InvalidStateError(PipelineError):
    """非法状态流转：如对终态运行再次取消、版本回滚目标非法。"""


class ValidationError(PipelineError):
    """定义非法：如依赖环、依赖不存在的步骤。"""
