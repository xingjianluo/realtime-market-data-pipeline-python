"""服务配置：窗口、迟到、容量等确定性参数。"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    window_size_ms: int = 1000
    allowed_lateness_ms: int = 500
    max_future_skew_ms: int = 10_000
    max_events_per_window: int = 1000
    max_submit_batch: int = 500
    data_dir: str = "data"


DEFAULT_CONFIG = Config()
