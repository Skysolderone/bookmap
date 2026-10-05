"""足迹图核心逻辑：纯计算，无 IO，便于单元测试。

数值约定：
  - 价格统一换算为整数 tick（价格 / tickSize），数量换算为整数单位（数量 / stepSize 精度），
    全程整数运算，避免浮点误差。
  - 内存中保存 30 秒粒度的「基础槽」（Slot），每个槽内按 tick 记录主动买/卖量和笔数，
    以及槽内的开高低收、笔数、槽内累计 delta 的极值。任意 30s 整数倍周期、任意 tick 整数倍分桶
    都可以由基础槽精确合成，切换周期/分桶无需重新请求币安。
  - 已收盘的槽可以「冻结」成一个紧凑的整数数组（约为字典存储的 1/4 内存），用于多天历史。
"""
from __future__ import annotations

import math
from array import array
from decimal import Decimal

SLOT_MS = 30_000

INTERVALS = {"30s": 1, "1m": 2, "5m": 10, "15m": 30}  # 周期 -> 包含的基础槽数量

_CACHE_MAX = 200_000


def decimals_of(s: str) -> int:
    """'0.010' -> 2, '1' -> 0, '0.10' -> 1"""
    d = Decimal(s).normalize()
    exp = d.as_tuple().exponent
    return max(0, -exp) if isinstance(exp, int) else 0


def to_scaled(s: str, dec: int) -> int:
    """把十进制字符串精确换算为 int(s * 10**dec)，多余的小数位必须全是 0。"""
    neg = s.startswith("-")
    if neg:
        s = s[1:]
    ip, _, fp = s.partition(".")
    if len(fp) > dec:
        if fp[dec:].strip("0"):
            raise ValueError(f"{s!r} 超出 {dec} 位小数精度")
        fp = fp[:dec]
    v = int(ip or "0") * 10**dec + (int(fp.ljust(dec, "0")) if dec else 0)
    return -v if neg else v


def is_taker_buy(m: bool) -> bool:
    """aggTrade 的 m = 买方是否为挂单方。m=True -> 主动卖出；m=False -> 主动买入。"""
    return not m


class SymbolSpec:
    """品种精度信息（来自 exchangeInfo）。价格/数量字符串的换算结果做了缓存（成交价高度重复）。"""

    def __init__(self, symbol: str, tick_size: str, step_size: str):
        self.symbol = symbol
        self.tick_size = tick_size
        self.step_size = step_size
        self.price_dec = decimals_of(tick_size)
        self.qty_dec = decimals_of(step_size)
        self.tick_units = to_scaled(tick_size, self.price_dec)  # tick 在 10**price_dec 刻度下的整数值
        self._pc: dict[str, int] = {}
        self._qc: dict[str, int] = {}

    def __getstate__(self):  # 跨进程传递时不带缓存
        return {k: v for k, v in self.__dict__.items() if k not in ("_pc", "_qc")}

    def __setstate__(self, st):
        self.__dict__.update(st)
        self._pc, self._qc = {}, {}

    def price_ticks(self, p: str) -> int:
        v = self._pc.get(p)
        if v is None:
            if len(self._pc) > _CACHE_MAX:
                self._pc.clear()
            v = self._pc[p] = to_scaled(p, self.price_dec) // self.tick_units
        return v

    def qty_units(self, q: str) -> int:
        v = self._qc.get(q)
        if v is None:
            if len(self._qc) > _CACHE_MAX:
                self._qc.clear()
            v = self._qc[q] = to_scaled(q, self.qty_dec)
        return v

    def ticks_to_decimal(self, ticks: int) -> Decimal:
        return Decimal(ticks * self.tick_units).scaleb(-self.price_dec)

    def meta(self) -> dict:
        return {
            "symbol": self.symbol,
            "tickSize": self.tick_size,
            "stepSize": self.step_size,
            "priceDec": self.price_dec,
            "qtyDec": self.qty_dec,
            "tickUnits": self.tick_units,
        }


def bucket_floor(price_ticks: int, bucket_ticks: int) -> int:
    """价格向下取整到 bucket 的整数倍（整数运算，负数同样向下取整）。"""
    return price_ticks // bucket_ticks * bucket_ticks


def nice_number(x: float) -> Decimal:
    """取最接近（对数意义）的 1/2/5 × 10^k。"""
    if x <= 0:
        raise ValueError("x must be positive")
    k = math.floor(math.log10(x))
    best = None
    for kk in (k - 1, k, k + 1):
        for m in (1, 2, 5):
            c = Decimal(m).scaleb(kk)
            d = abs(math.log10(float(c)) - math.log10(x))
            if best is None or d < best[0]:
                best = (d, c)
    return best[1]


def bucket_ticks_for(value: Decimal, spec: SymbolSpec) -> int:
    """把价格间距换算成 tick 数（至少 1 tick）。"""
    t = (Decimal(value) / Decimal(spec.tick_size)).to_integral_value()
    return max(1, int(t))


def default_bucket_ticks(spec: SymbolSpec, price: float) -> int:
    if spec.symbol == "ETHUSDT":
        return bucket_ticks_for(Decimal(1), spec)
    return bucket_ticks_for(nice_number(price * 0.0003), spec)


def bucket_choices(spec: SymbolSpec, price: float) -> list[int]:
    """可选分桶（tick 数）：1 tick 起，1/2/5 序列，最大约为现价的 1%。"""
    tick = Decimal(spec.tick_size)
    out = {1}
    hi = Decimal(str(price)) * Decimal("0.01")
    k = tick.adjusted()
    while True:
        stop = False
        for m in (1, 2, 5):
            v = Decimal(m).scaleb(k)
            if v > hi:
                stop = True
                break
            if v >= tick and (v / tick) == (v / tick).to_integral_value():
                out.add(int(v / tick))
        if stop:
            break
        k += 1
    return sorted(out)


class Slot:
    """30 秒基础槽。

    cells：活跃时为 {tick: [主动买, 主动卖, 买笔数, 卖笔数]}；冻结后为 None，数据在
    packed（array('q')，每格 5 个整数：tick, 买, 卖, 买笔数, 卖笔数）。
    cum/dmax/dmin 是槽内从 0 开始的累计 delta 及其极值（每笔成交后取值）。
    """

    __slots__ = ("o", "h", "l", "c", "buy", "sell", "n", "cum", "dmax", "dmin", "cells", "packed")

    def __init__(self, pt: int):
        self.o = self.h = self.l = self.c = pt
        self.buy = self.sell = self.n = self.cum = 0
        self.dmax = -(1 << 62)
        self.dmin = 1 << 62
        self.cells: dict[int, list[int]] | None = {}
        self.packed: array | None = None

    def __getstate__(self):
        if self.cells is not None:
            self.freeze()
        return tuple(getattr(self, k) for k in self.__slots__)

    def __setstate__(self, st):
        for k, v in zip(self.__slots__, st):
            setattr(self, k, v)

    def freeze(self) -> None:
        if self.cells is None:
            return
        a = array("q")
        for pt, v in self.cells.items():
            a.extend((pt, v[0], v[1], v[2], v[3]))
        self.packed, self.cells = a, None

    def thaw(self) -> None:
        if self.cells is not None:
            return
        a = self.packed
        self.cells = {a[i]: [a[i + 1], a[i + 2], a[i + 3], a[i + 4]] for i in range(0, len(a), 5)}
        self.packed = None

    def iter_cells(self):
        """逐格返回 (tick, 买, 卖, 买笔数, 卖笔数)。"""
        if self.cells is not None:
            for pt, v in self.cells.items():
                yield pt, v[0], v[1], v[2], v[3]
        else:
            a = self.packed
            for i in range(0, len(a), 5):
                yield a[i], a[i + 1], a[i + 2], a[i + 3], a[i + 4]

    def add(self, pt: int, qty: int, buy: bool) -> None:
        if self.cells is None:
            self.thaw()
        if pt > self.h:
            self.h = pt
        if pt < self.l:
            self.l = pt
        self.c = pt
        self.n += 1
        cell = self.cells.get(pt)
        if cell is None:
            cell = self.cells[pt] = [0, 0, 0, 0]
        if buy:
            self.buy += qty
            self.cum += qty
            cell[0] += qty
            cell[2] += 1
        else:
            self.sell += qty
            self.cum -= qty
            cell[1] += qty
            cell[3] += 1
        if self.cum > self.dmax:
            self.dmax = self.cum
        if self.cum < self.dmin:
            self.dmin = self.cum


class Book:
    """单个品种的成交聚合簿。成交必须按聚合成交 ID 递增顺序喂入（由上层保证无重复、无缺口）。"""

    def __init__(self, spec: SymbolSpec):
        self.spec = spec
        self.slots: dict[int, Slot] = {}
        self.first_id: int | None = None
        self.last_id: int | None = None
        self.last_T: int | None = None
        self.last_price: int | None = None
        self.dirty: set[int] = set()
        self.trades = 0

    def apply(self, a: int, T: int, pt: int, qty: int, buy: bool) -> bool:
        """应用一笔成交；ID 不大于 last_id 的视为重复，返回 False。"""
        if self.last_id is not None and a <= self.last_id:
            return False
        sid = T // SLOT_MS
        slot = self.slots.get(sid)
        if slot is None:
            slot = self.slots[sid] = Slot(pt)
        slot.add(pt, qty, buy)
        if self.first_id is None:
            self.first_id = a
        self.last_id = a
        self.last_T = T
        self.last_price = pt
        self.dirty.add(sid)
        self.trades += 1
        return True

    def apply_raw(self, d: dict) -> bool:
        """应用币安 aggTrade 原始字典（REST 或 WS data），只用 a/p/q/T/m。"""
        s = self.spec
        return self.apply(int(d["a"]), int(d["T"]), s.price_ticks(d["p"]), s.qty_units(d["q"]), is_taker_buy(d["m"]))

    def merge_newer(self, other: "Book") -> int:
        """把一段更新的、已经聚合好的成交（如某一天的历史包）接到末尾。

        返回 other 与当前末尾之间缺失的 ID 数（0 表示首尾相接）。other 中不大于 last_id 的部分不会出现：
        调用方按 ID 顺序加载，且历史包按天切分，不会和已有数据重叠。
        """
        if other.last_id is None:
            return 0
        missing = 0
        if self.last_id is not None:
            if other.first_id <= self.last_id:
                raise ValueError("merge_newer: 数据与已有部分重叠")
            missing = other.first_id - self.last_id - 1
        for sid in other.slots:
            if sid in self.slots:  # 按 UTC 天切分时不会发生（午夜是 30s 的整数倍）
                raise ValueError("merge_newer: 槽重叠")
        self.slots.update(other.slots)
        self.dirty.update(other.slots)
        if self.first_id is None:
            self.first_id = other.first_id
        self.last_id, self.last_T, self.last_price = other.last_id, other.last_T, other.last_price
        self.trades += other.trades
        return missing

    def pop_dirty(self) -> set[int]:
        d, self.dirty = self.dirty, set()
        return d

    def freeze_before(self, ms: int) -> int:
        """冻结开始时间早于 ms 的槽，返回冻结数量。"""
        cut = ms // SLOT_MS
        n = 0
        for sid, s in self.slots.items():
            if sid < cut and s.cells is not None:
                s.freeze()
                n += 1
        return n

    def delta_before(self, ms: int) -> int:
        """开始时间早于 ms 的全部槽的 delta 之和（用作 CVD 起点）。"""
        cut = ms // SLOT_MS
        return sum(s.cum for sid, s in self.slots.items() if sid < cut)

    def trim(self, keep_from_ms: int) -> int:
        cut = keep_from_ms // SLOT_MS
        old = [k for k in self.slots if k < cut]
        for k in old:
            del self.slots[k]
        return len(old)


def _bar_from_slots(slots: list[Slot], t: int, bucket_ticks: int) -> dict:
    first = slots[0]
    o, h, l, c = first.o, first.h, first.l, first.c
    buy = sell = n = 0
    prefix = 0
    dmax = -(1 << 62)
    dmin = 1 << 62
    cells: dict[int, list[int]] = {}
    for s in slots:
        if s.h > h:
            h = s.h
        if s.l < l:
            l = s.l
        c = s.c
        buy += s.buy
        sell += s.sell
        n += s.n
        if prefix + s.dmax > dmax:
            dmax = prefix + s.dmax
        if prefix + s.dmin < dmin:
            dmin = prefix + s.dmin
        prefix += s.cum
        for pt, b, sl, nb, ns in s.iter_cells():
            k = pt // bucket_ticks * bucket_ticks
            cell = cells.get(k)
            if cell is None:
                cells[k] = [b, sl, nb, ns]
            else:
                cell[0] += b
                cell[1] += sl
                cell[2] += nb
                cell[3] += ns
    # POC：成交量最大的分桶；并列时取价格较低者，保证结果确定
    poc = max(cells.items(), key=lambda kv: (kv[1][0] + kv[1][1], -kv[0]))[0]
    return {
        "t": t, "o": o, "h": h, "l": l, "c": c,
        "v": buy + sell, "buy": buy, "sell": sell, "delta": buy - sell,
        "dmax": dmax, "dmin": dmin, "n": n, "poc": poc, "cvd": 0,
        "cells": [[k, v[0], v[1], v[2], v[3]] for k, v in sorted(cells.items(), reverse=True)],
    }


def build_bars(
    book: Book,
    interval_slots: int,
    bucket_ticks: int,
    bar_ids: set[int] | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
    limit: int | None = None,
    cvd_base: int = 0,
) -> list[dict]:
    """由基础槽合成 K 线。

    - bar_ids：只合成这些 K 线（实时推送用，按 ID 直接查槽，不扫描全部数据）。
    - start_ms / end_ms：按开盘时间过滤（end 为开区间）；limit：只取最后 limit 根。
    - cvd_base：第一根之前的累计 delta，返回的每根 cvd = cvd_base + 本根及之前的 delta 之和。

    每根 K 线：t 开盘时间(ms)，o/h/l/c(tick)，v/buy/sell/delta/dmax/dmin(数量单位)，n 笔数，poc(分桶下沿 tick)，
    cvd，cells=[[分桶下沿tick, 主动买, 主动卖, 买笔数, 卖笔数], ...] 价格从高到低。
    """
    k = interval_slots
    slots = book.slots
    groups: list[tuple[int, list[Slot]]] = []
    if bar_ids is not None:
        for bid in sorted(bar_ids):
            g = [slots[sid] for sid in range(bid * k, bid * k + k) if sid in slots]
            if g:
                groups.append((bid, g))
    else:
        lo = None if start_ms is None else start_ms // SLOT_MS
        hi = None if end_ms is None else end_ms // SLOT_MS
        cur_bid, cur = None, None
        for sid in sorted(slots):
            if (lo is not None and sid < lo) or (hi is not None and sid >= hi):
                continue
            bid = sid // k
            if bid != cur_bid:
                cur_bid, cur = bid, []
                groups.append((bid, cur))
            cur.append(slots[sid])
        if limit is not None and len(groups) > limit:
            groups = groups[-limit:]
    bars = []
    cvd = cvd_base
    for bid, g in groups:
        b = _bar_from_slots(g, bid * k * SLOT_MS, bucket_ticks)
        cvd += b["delta"]
        b["cvd"] = cvd
        bars.append(b)
    return bars
