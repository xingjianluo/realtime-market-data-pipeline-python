"""本地持久化：追加式事件日志、聚合快照与消费位点。

用本地文件模拟数据源日志与消费位点：
- events.log：追加式 JSONL，每行一条「已确认」记录（接受或拒绝，重投可追溯）。
- snapshot.json：窗口聚合/去重索引/水位线的原子快照，重启后据此恢复，日志兜底。
- offsets.json：各来源已确认的最大事件序号（消费位点），原子覆写。

所有写操作 flush + fsync；快照与位点通过临时文件 + os.replace 原子落盘。
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class LocalStorage:
    """以本地文件模拟数据源日志与消费位点存储。"""

    def __init__(self, data_dir: str) -> None:
        self._dir = Path(data_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._log_path = self._dir / "events.log"
        self._snapshot_path = self._dir / "snapshot.json"
        self._offsets_path = self._dir / "offsets.json"
        self._log_file = open(self._log_path, "a", encoding="utf-8")

    # ---- 内部工具 -------------------------------------------------------

    @staticmethod
    def _atomic_write_json(path: Path, payload: Any) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)

    # ---- 事件日志 -------------------------------------------------------

    def append_log(self, record: dict[str, Any]) -> int:
        """追加一条已确认记录，返回该记录在日志中的 1 基序列号。"""
        seq = self.log_size() + 1
        record = {"seq": seq, **record}
        self._log_file.write(
            json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        )
        self._log_file.flush()
        os.fsync(self._log_file.fileno())
        return seq

    def log_size(self) -> int:
        """当前日志行数。"""
        try:
            with open(self._log_path, "r", encoding="utf-8") as fh:
                return sum(1 for _ in fh)
        except FileNotFoundError:
            return 0

    def load_log(self) -> list[dict[str, Any]]:
        """读出全部日志记录（跳过尾部因崩溃产生的残缺行）。"""
        records: list[dict[str, Any]] = []
        if not self._log_path.exists():
            return records
        with open(self._log_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    # 追加到一半的残缺行：忽略，不影响此前已 fsync 的完整记录。
                    continue
        return records

    # ---- 聚合快照 -------------------------------------------------------

    def save_snapshot(self, snapshot: dict[str, Any]) -> None:
        """原子写入聚合快照。"""
        self._atomic_write_json(self._snapshot_path, snapshot)

    def load_snapshot(self) -> dict[str, Any] | None:
        """读取快照；不存在时返回 None。"""
        if not self._snapshot_path.exists():
            return None
        with open(self._snapshot_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    # ---- 消费位点 -------------------------------------------------------

    def save_offsets(self, offsets: dict[str, int]) -> None:
        """原子写入全部来源消费位点。"""
        self._atomic_write_json(self._offsets_path, dict(sorted(offsets.items())))

    def load_offsets(self) -> dict[str, int]:
        """读取全部来源消费位点。"""
        if not self._offsets_path.exists():
            return {}
        with open(self._offsets_path, "r", encoding="utf-8") as fh:
            return {str(k): int(v) for k, v in json.load(fh).items()}

    def close(self) -> None:
        """关闭日志文件句柄。"""
        self._log_file.close()
