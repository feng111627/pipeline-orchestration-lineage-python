"""服务统一的业务错误类型。

所有可预期的业务拒绝都抛出这里的类型，入口层（HTTP / CLI / 测试）
据此把"非法请求"与"系统故障"区分开。
"""


class PipelineError(Exception):
    """所有业务错误的基类。"""


class NotFoundError(PipelineError):
    """引用的管道、版本或运行不存在。"""


class ValidationError(PipelineError):
    """定义本身非法（如依赖环、未知依赖、空步骤）。"""


class ConflictError(PipelineError):
    """并发/重复冲突：已有活动运行或幂等键已被占用。"""


class InvalidTransitionError(PipelineError):
    """非法状态流转（如取消已结束的运行、重复终态化）。"""


class ResumeNotPossibleError(PipelineError):
    """续跑请求被明确拒绝：源运行未结束、已全部成功或没有可续跑步骤。"""
