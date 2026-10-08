"""本地持久化存储：用文件模拟数据源消费位点与状态持久化。

文件布局（均位于 data_dir）：
- events.log      已确认事件的只追加 WAL（每行一条 JSON）；
- state.json      聚合/去重/水位线状态快照（原子替换）；
- offsets.json    各来源的消费位点（source -> 已提交最大 offset）；
- dead_letters.log 被拒绝事件的只追加日志，支持重新投递后移除。

持久化顺序：先写 WAL 并 fsync，再更新内存聚合，最后原子写状态快照，
保证服务重启后"已确认的事件与聚合状态不重复、不丢失"。

金额字段（Decimal）以**精确十进制 JSON 数字**落盘（不经过 float），
读回时所有小数还原为 Decimal，杜绝精度漂移；旧版 float 快照/WAL 也可直接读入。
"""

from __future__ import annotations

import json
import os
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

# dumps 先把 Decimal 标记成带哨兵的字符串；json.dumps 会把哨兵中的控制字符
# 转义成字面量 \u0000，故按输出态的转义序列匹配并还原为裸 JSON 数字 token，
# 全程不经过 float。用户数据里即便出现 "\u0000" 文本，反斜杠也会被二次转义，
# 不可能与该模式混淆。loads 用 parse_float=Decimal 精确还原。
_DEC_TAG = "\x00DEC\x00"
_DEC_TAG_OUT = r"\u0000DEC\u0000"
_DEC_TOKEN_RE = re.compile(r'"' + re.escape(_DEC_TAG_OUT) + r'([0-9.+\-eE]+)"')


def dumps(obj: Any) -> str:
    """JSON 序列化，Decimal 输出为精确数字字面量。"""

    def _default(value: Any) -> Any:
        if isinstance(value, Decimal):
            return f"{_DEC_TAG}{value}"
        if isinstance(value, (set, tuple)):
            return list(value)
        raise TypeError(f"不可序列化的类型: {type(value).__name__}")

    text = json.dumps(obj, ensure_ascii=False, sort_keys=True, default=_default)
    return _DEC_TOKEN_RE.sub(lambda m: m.group(1), text)


def loads(text: str) -> Any:
    """JSON 反序列化，所有小数 token 精确还原为 Decimal。"""
    return json.loads(text, parse_float=Decimal)


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
        self._wal_f.write(dumps(record))
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
                    records.append(loads(line))
        return records

    # ---- 聚合状态快照 ----------------------------------------------------
    def save_state(self, state: dict[str, Any]) -> None:
        path = self.data_dir / self.STATE_NAME
        tmp = self.data_dir / (self.STATE_NAME + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(dumps(state))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(self.data_dir)

    def load_state(self) -> dict[str, Any] | None:
        path = self.data_dir / self.STATE_NAME
        if not path.exists():
            return None
        with open(path, encoding="utf-8") as f:
            return loads(f.read())

    # ---- 消费位点 --------------------------------------------------------
    def save_offsets(self, offsets: dict[str, int]) -> None:
        path = self.data_dir / self.OFFSETS_NAME
        tmp = self.data_dir / (self.OFFSETS_NAME + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(dumps(offsets))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(self.data_dir)

    def load_offsets(self) -> dict[str, int]:
        path = self.data_dir / self.OFFSETS_NAME
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return loads(f.read())

    # ---- 拒绝记录（死信，可重投）----------------------------------------
    def append_dead_letter(self, record: dict[str, Any]) -> None:
        assert self._dead_f is not None, "storage 尚未 setup"
        self._dead_f.write(dumps(record))
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
                    records.append(loads(line))
        return records

    def remove_dead_letter(self, event_id: str,
                           source: str | None = None) -> None:
        """按 event_id 删除死信；给定 source 时只删该来源内的条目。

        多来源 event_id 可能撞车，删除必须限定来源作用域，避免误删他源死信。
        """
        path = self.data_dir / self.DEAD_NAME
        if not path.exists():
            return

        def _matches(rec: dict[str, Any]) -> bool:
            if rec.get("event_id") != event_id:
                return False
            if source is None:
                return True
            return rec.get("source") == source

        kept = [r for r in self.load_dead_letters() if not _matches(r)]
        tmp = self.data_dir / (self.DEAD_NAME + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for r in kept:
                f.write(dumps(r))
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(self.data_dir)
