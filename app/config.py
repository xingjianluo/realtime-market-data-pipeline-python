"""服务配置：窗口、迟到、容量等确定性参数。"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    window_size_ms: int = 1000
    allowed_lateness_ms: int = 500
    max_future_skew_ms: int = 10_000
    max_events_per_window: int = 1000
    max_submit_batch: int = 500
    # 服务级总闸：整个服务生命周期内累计受理事件数上限
    # （跨来源、跨窗口、跨关闭窗口一起计数，含重启后从持久化恢复的占用）。
    # 设为 None 表示不限制；到顶后新事件以 SERVICE_FULL 拒绝。
    max_accepted_events: int | None = 100_000
    data_dir: str = "data"


DEFAULT_CONFIG = Config()
