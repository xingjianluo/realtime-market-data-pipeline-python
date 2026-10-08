"""本地持久化存储：用文件模拟数据源消费位点与状态持久化。

文件布局（均位于 data_dir）：
- events.log      已确认事件的只追加 WAL（每行一条 JSON）；
- state.json      聚合/去重/水位线状态快照（原子替换）；
- offsets.json    各来源的消费位点（source -> 已提交最大 offset）；
- dead_letters.log 被拒绝事件的只追加日志，支持重新投递后移除。

持久化顺序：先写 WAL 并 fsync，再更新内存聚合，最后原子写状态快照，
保证服务重启后"已确认的事件与聚合状态不重复、不丢失"。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class LocalStorage:
    """基于本地文件的确定性持久层。"""

    WAL_NAME = "events.log"
    STATE_NAME = "state.json"
    OFFSETS_NAME = "offsets.json"
    DEAD_NAME = "dead_letters.log"

    def __init__(self, data_dir: str) -> None:
        self.data_dir = Path(data_dir)
        self._wal_f: Any = None
        self._dead_f: Any = None

    # ---- 生命周期 -------------------------------------------------------
    def setup(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # 行缓冲追加；fsync 由 append 方显式触发
        self._wal_f = open(self.data_dir / self.WAL_NAME, "a", encoding="utf-8",
                           buffering=1)
        self._dead_f = open(self.data_dir / self.DEAD_NAME, "a", encoding="utf-8",
                            buffering=1)

    def close(self) -> None:
        for f in (self._wal_f, self._dead_f):
            if f is not None:
                f.flush()
                os.fsync(f.fileno())
                f.close()
        self._wal_f = None
        self._dead_f = None

    # ---- 已确认事件 WAL -------------------------------------------------
    def append_wal(self, record: dict[str, Any]) -> None:
        assert self._wal_f is not None, "storage 尚未 setup"
        self._wal_f.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
        self._wal_f.write("\n")
        self._wal_f.flush()
        os.fsync(self._wal_f.fileno())

    def load_wal(self) -> list[dict[str, Any]]:
        path = self.data_dir / self.WAL_NAME
        if not path.exists():
            return []
        records: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    # ---- 聚合状态快照 ----------------------------------------------------
    def save_state(self, state: dict[str, Any]) -> None:
        path = self.data_dir / self.STATE_NAME
        tmp = self.data_dir / (self.STATE_NAME + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(self.data_dir)

    def load_state(self) -> dict[str, Any] | None:
        path = self.data_dir / self.STATE_NAME
        if not path.exists():
            return None
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    # ---- 消费位点 --------------------------------------------------------
    def save_offsets(self, offsets: dict[str, int]) -> None:
        path = self.data_dir / self.OFFSETS_NAME
        tmp = self.data_dir / (self.OFFSETS_NAME + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(offsets, f, ensure_ascii=False, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(self.data_dir)

    def load_offsets(self) -> dict[str, int]:
        path = self.data_dir / self.OFFSETS_NAME
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    # ---- 拒绝记录（死信，可重投）----------------------------------------
    def append_dead_letter(self, record: dict[str, Any]) -> None:
        assert self._dead_f is not None, "storage 尚未 setup"
        self._dead_f.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
        self._dead_f.write("\n")
        self._dead_f.flush()
        os.fsync(self._dead_f.fileno())

    def load_dead_letters(self) -> list[dict[str, Any]]:
        path = self.data_dir / self.DEAD_NAME
        if not path.exists():
            return []
        records: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def remove_dead_letter(self, event_id: str, source: str = "") -> None:
        """移除死信条目；多来源编号撞号时用 source 精确定位。

        source 为空串时退化为仅按 event_id 匹配（兼容旧死信记录无 source）。
        """
        path = self.data_dir / self.DEAD_NAME
        if not path.exists():
            return

        def _match(rec: dict[str, Any]) -> bool:
            if rec.get("event_id") != event_id:
                return False
            if source == "":
                return True
            return rec.get("source", "") == source

        kept = [r for r in self.load_dead_letters() if not _match(r)]
        tmp = self.data_dir / (self.DEAD_NAME + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for r in kept:
                f.write(json.dumps(r, ensure_ascii=False, sort_keys=True))
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(self.data_dir)
