"""币安 USDⓈ-M 合约公开行情访问：REST（限速）、WebSocket（重连）、按 ID 去重与缺口回补。

只使用公开行情接口，不涉及任何 API key / 账户 / 下单。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from datetime import date, datetime, timezone
from typing import Awaitable, Callable

import aiohttp

from fp_core import Book, SymbolSpec

log = logging.getLogger("binance")

REST_BASE = "https://fapi.binance.com"
# 2026 年拆分后的行情入口，aggTrade 属于 /market
WS_MARKET = "wss://fstream.binance.com/market/stream"

WEIGHT_LIMIT = 1200  # 自我约束阈值（币安 IP 上限为 2400/分钟）
AGG_WEIGHT = 20
AGG_LIMIT = 1000
REST_HISTORY_MS = 47 * 3600 * 1000  # aggTrades 只能查最近 2 天（留 1 小时余量）
WS_MAX_AGE = 23 * 3600 + 30 * 60  # 单连接最长 24h，提前主动换线
WS_IDLE_TIMEOUT = 240  # 连续这么多秒收不到任何数据就主动断开重连（同 Flowsurface 对合约的取值）


def resolve_proxy(cli: str | None) -> str | None:
    """--proxy 优先，其次 HTTPS_PROXY / ALL_PROXY（大小写均可）。"""
    if cli:
        return cli
    for k in ("HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
        v = os.environ.get(k)
        if v:
            return v
    return None


def make_session(proxy: str | None) -> tuple[aiohttp.ClientSession, str | None]:
    """返回 (session, 每次请求要带的 proxy 参数)。

    http(s):// 代理用 aiohttp 原生 proxy 参数；socks5:// / socks4:// 走 aiohttp-socks 的 connector。
    """
    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10)
    if proxy and proxy.lower().startswith("socks"):
        try:
            from aiohttp_socks import ProxyConnector
        except ImportError as e:
            raise SystemExit("使用 SOCKS 代理需要安装 aiohttp-socks：pip install aiohttp-socks") from e
        # socks5h:// 语义（远端解析域名）在 aiohttp-socks 里用 rdns=True
        url = proxy.replace("socks5h://", "socks5://", 1)
        conn = ProxyConnector.from_url(url, rdns=True)
        return aiohttp.ClientSession(connector=conn, timeout=timeout), None
    return aiohttp.ClientSession(timeout=timeout), proxy


class RateLimitError(Exception):
    pass


class RestRangeError(Exception):
    """-4166：aggTrades 只能查最近 2 天（fromId 指向更早的 ID 也会报这个错）。"""


class BinanceRest:
    def __init__(self, session: aiohttp.ClientSession, proxy: str | None):
        self.session = session
        self.proxy = proxy
        self.used = 0
        self.used_minute = 0
        self._lock = asyncio.Lock()

    async def _wait_weight(self, weight: int) -> None:
        minute = int(time.time() // 60)
        if minute != self.used_minute:
            self.used, self.used_minute = 0, minute
        if self.used + weight > WEIGHT_LIMIT:
            wait = (minute + 1) * 60 - time.time() + 0.5
            log.info("REST 权重 %d 已接近上限，等待 %.1fs 到下一分钟", self.used, wait)
            await asyncio.sleep(wait)
            self.used, self.used_minute = 0, int(time.time() // 60)

    async def get(self, path: str, params: dict | None = None, weight: int = 1):
        # 串行化请求，权重统计才准确；行情回补本身也是顺序翻页
        async with self._lock:
            for attempt in range(2):  # 429/418 按 Retry-After 等一次后最多再试一次，不反复重试
                await self._wait_weight(weight)
                async with self.session.get(REST_BASE + path, params=params, proxy=self.proxy) as r:
                    used = r.headers.get("X-MBX-USED-WEIGHT-1M")
                    if used is not None:
                        self.used, self.used_minute = int(used), int(time.time() // 60)
                    if r.status in (429, 418):
                        ra = int(r.headers.get("Retry-After", "60"))
                        log.warning("REST 返回 %d（%s），按 Retry-After 等待 %ds", r.status,
                                    "限频" if r.status == 429 else "IP 已被封禁", ra)
                        await asyncio.sleep(ra)
                        continue
                    if r.status >= 400:
                        body = await r.text()
                        if '"code":-4166' in body.replace(" ", ""):
                            raise RestRangeError(body)
                        raise aiohttp.ClientResponseError(
                            r.request_info, r.history, status=r.status, message=body)
                    return await r.json()
            raise RateLimitError(f"{path} 多次触发限频，放弃")

    async def exchange_info(self) -> dict:
        return await self.get("/fapi/v1/exchangeInfo", weight=1)

    async def ticker_price(self, symbol: str) -> float:
        d = await self.get("/fapi/v1/ticker/price", {"symbol": symbol}, weight=1)
        return float(d["price"])

    async def agg_trades(self, symbol: str, *, from_id: int | None = None,
                         start_time: int | None = None, end_time: int | None = None,
                         limit: int = AGG_LIMIT) -> list[dict]:
        p: dict = {"symbol": symbol, "limit": limit}
        if from_id is not None:
            # fromId 不能与 startTime/endTime 同时使用
            p["fromId"] = from_id
        else:
            if start_time is not None:
                p["startTime"] = start_time
            if end_time is not None:
                p["endTime"] = end_time
        return await self.get("/fapi/v1/aggTrades", p, weight=AGG_WEIGHT)

    async def klines(self, symbol: str, interval: str, start_time: int, end_time: int, limit: int = 100):
        w = 1 if limit < 100 else 2 if limit < 500 else 5 if limit <= 1000 else 10
        return await self.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval,
                                                  "startTime": start_time, "endTime": end_time,
                                                  "limit": limit}, weight=w)


def load_specs(info: dict, symbols: list[str]) -> dict[str, SymbolSpec]:
    by = {s["symbol"]: s for s in info["symbols"]}
    out = {}
    for sym in symbols:
        if sym not in by:
            raise SystemExit(f"exchangeInfo 中找不到品种 {sym}")
        f = {x["filterType"]: x for x in by[sym]["filters"]}
        out[sym] = SymbolSpec(sym, f["PRICE_FILTER"]["tickSize"], f["LOT_SIZE"]["stepSize"])
    return out


FetchFn = Callable[..., Awaitable[list[dict]]]
DayLoader = Callable[[date], Awaitable["Book | None"]]
DAY_MS = 86_400_000


class SymbolFeed:
    """单品种成交序列化：保证喂给 Book 的成交按 ID 连续、不重复。

    - 历史日包（--backfill-days）：今天（UTC）之前的每一天从 data.binance.vision 加载，按 ID 首尾衔接；
      某天的包还没生成且仍在 REST 可查范围内时，从那天开始改用 REST。
    - 启动回补：有历史包时从最后一个 ID 起用 fromId 翻页追到最新；否则从 now-N 分钟按 startTime 取第一页，
      之后用 fromId 翻页。
    - 实时：WS 消息先进队列；处理时若发现 ID 跳号（断线/丢包），先用 fromId 通过 REST 补齐再继续。
    - 断线超过 REST 可查范围（2 天）则放弃补缺，按时间重新回补。
    """

    def __init__(self, book: Book, fetch: FetchFn, *, backfill_ms: int, retention_ms: int,
                 backfill_days: int = 0, load_day: DayLoader | None = None,
                 on_trade: Callable[[dict], None] | None = None, now_ms: Callable[[], int] | None = None):
        self.book = book
        self.symbol = book.spec.symbol
        self.fetch = fetch
        self.backfill_ms = backfill_ms
        self.retention_ms = retention_ms
        self.backfill_days = backfill_days
        self.load_day = load_day
        self.queue: asyncio.Queue[dict] = asyncio.Queue()
        self.on_trade = on_trade
        self.now_ms = now_ms or (lambda: int(time.time() * 1000))
        self.state = "init"
        self.progress = ""
        self.gap_fills = 0
        self.gap_trades = 0
        self.lost = 0  # 交易所已查不到的缺失 ID 数

    def _apply(self, d: dict) -> None:
        if self.book.apply_raw(d) and self.on_trade:
            self.on_trade(d)

    def _apply_rows(self, rows: list[dict]) -> None:
        """顺序应用一页成交，页内 ID 跳号计入 lost。"""
        for d in rows:
            last = self.book.last_id
            if last is not None and d["a"] > last + 1:
                self.lost += d["a"] - last - 1
                log.warning("%s ID %d..%d 缺失", self.symbol, last + 1, d["a"] - 1)
            self._apply(d)

    def _set_progress(self, T: int, start_ms: int) -> None:
        span = max(1, self.now_ms() - start_ms)
        self.progress = f"{min(100, max(0, 100 * (T - start_ms) // span))}%"

    async def backfill(self, start_ms: int, until_id: int | None = None) -> None:
        """按时间回补：第一页用 startTime，之后用 fromId 翻页直到追上最新（或到达 until_id）。"""
        self.state = "backfill"
        start_ms = max(start_ms, self.now_ms() - REST_HISTORY_MS)
        rows = await self.fetch(self.symbol, start_time=start_ms)
        while rows:
            self._apply_rows(rows)
            self._set_progress(rows[-1]["T"], start_ms)
            if len(rows) < AGG_LIMIT or (until_id is not None and self.book.last_id >= until_id):
                break
            rows = await self.fetch(self.symbol, from_id=self.book.last_id + 1)
        self.progress = ""

    async def catch_up(self) -> None:
        """从 last_id+1 起用 fromId 翻页追到最新。"""
        self.state = "backfill"
        start = self.book.last_T or self.now_ms()
        while True:
            rows = await self.fetch(self.symbol, from_id=self.book.last_id + 1)
            if not rows:
                break
            self._apply_rows(rows)
            self._set_progress(rows[-1]["T"], start)
            if len(rows) < AGG_LIMIT:
                break
        self.progress = ""

    async def load_history(self) -> None:
        now = self.now_ms()
        today0 = now // DAY_MS * DAY_MS
        rest_from = None
        for i in range(self.backfill_days, 0, -1):
            day0 = today0 - i * DAY_MS
            day = datetime.fromtimestamp(day0 / 1000, timezone.utc).date()
            self.state, self.progress = "history", f"{day} ({self.backfill_days - i + 1}/{self.backfill_days})"
            try:
                part = await self.load_day(day)
            except Exception as e:  # noqa: BLE001 下载/解析失败按「不可用」处理
                log.warning("%s %s 历史包加载失败：%r", self.symbol, day, e)
                part = None
            if part is None:
                if day0 >= now - REST_HISTORY_MS:
                    log.info("%s %s 的历史包尚未生成，从这天起改用 REST", self.symbol, day)
                    rest_from = day0
                    break
                log.warning("%s %s 的历史包不可用且超出 REST 可查范围（2 天），跳过", self.symbol, day)
                continue
            missing = self.book.merge_newer(part)
            if missing:
                self.lost += missing
                log.warning("%s %s 与前一天之间缺 %d 个 ID", self.symbol, day, missing)
            log.info("%s %s 已加载 %d 条", self.symbol, day, part.trades)
        self.progress = ""
        if self.book.last_id is not None:
            try:
                await self.catch_up()
                return
            except RestRangeError:
                log.warning("%s 最后一个历史 ID 已超出 REST 可查范围，按时间回补，中间会有缺口", self.symbol)
        await self.backfill(rest_from if rest_from is not None else now - REST_HISTORY_MS)

    async def fill_gap(self, target_id: int) -> None:
        """补齐 (last_id, target_id) 之间缺失的成交。"""
        b = self.book
        if b.last_T is not None and self.now_ms() - b.last_T > REST_HISTORY_MS:
            await self._rebackfill(target_id)
            return
        self.state = "gapfill"
        start = b.last_id
        self.gap_fills += 1
        missing = 0
        try:
            while b.last_id < target_id - 1:
                rows = await self.fetch(self.symbol, from_id=b.last_id + 1)
                if not rows:
                    break
                for d in rows:
                    if d["a"] > b.last_id + 1:
                        missing += min(d["a"], target_id) - b.last_id - 1
                    self._apply(d)
        except RestRangeError:
            await self._rebackfill(target_id)
            return
        missing += max(0, target_id - 1 - b.last_id)
        if missing > 0:
            self.lost += missing
            log.warning("%s 缺口 %d 条在 REST 中也查不到", self.symbol, missing)
        filled = min(b.last_id, target_id - 1) - start - missing
        self.gap_trades += filled
        log.info("%s 补缺 %d 条（ID %d..%d）", self.symbol, filled, start + 1, target_id - 1)

    async def _rebackfill(self, target_id: int) -> None:
        log.warning("%s 断线超过 REST 可查范围，按时间重新回补（中间的成交无法找回）", self.symbol)
        await self.backfill(self.now_ms() - min(self.retention_ms, REST_HISTORY_MS), until_id=target_id - 1)

    async def run(self) -> None:
        if self.backfill_days > 0 and self.load_day is not None:
            await self.load_history()
        elif self.backfill_ms > 0:
            await self.backfill(self.now_ms() - self.backfill_ms)
        while True:
            self.state = "live"
            d = await self.queue.get()
            a = d["a"]
            last = self.book.last_id
            if last is not None and a <= last:
                continue
            if last is not None and a > last + 1:
                await self.fill_gap(a)
            self._apply(d)


class MarketStream:
    """合并订阅多个品种 aggTrade。

    - 断线指数退避重连（1s 起翻倍，最长 60s，±20% 抖动；连接稳定 60s 后退避复位）。
    - 无数据超时：连续 idle_timeout 秒没有收到任何行情消息就主动断开重连（服务端 ping 不算数据）。
    - 单连接满 23.5h 主动换线（币安单连接最长 24h）。
    - 断线期间的缺口由 SymbolFeed 发现 ID 跳号后用 REST 补齐。
    """

    def __init__(self, session: aiohttp.ClientSession, proxy: str | None, feeds: dict[str, SymbolFeed],
                 *, idle_timeout: float = WS_IDLE_TIMEOUT, url: str | None = None):
        self.session = session
        self.proxy = proxy
        self.feeds = feeds
        self.idle_timeout = idle_timeout
        self._url = url
        self.state = "connecting"
        self.connects = 0
        self.idle_reconnects = 0
        self.last_msg = 0.0

    @property
    def url(self) -> str:
        return self._url or WS_MARKET + "?streams=" + "/".join(f"{s.lower()}@aggTrade" for s in self.feeds)

    def _dispatch(self, raw: str) -> None:
        d = json.loads(raw).get("data")
        if d and d.get("e") == "aggTrade":
            f = self.feeds.get(d["s"])
            if f:
                f.queue.put_nowait(d)

    async def _session_once(self) -> None:
        started = time.monotonic()
        async with self.session.ws_connect(self.url, proxy=self.proxy, heartbeat=30, max_msg_size=0) as ws:
            self.state = "live"
            self.connects += 1
            log.info("行情流已连接（第 %d 次）", self.connects)
            last_data = time.monotonic()
            while True:
                remaining = self.idle_timeout - (time.monotonic() - last_data)
                try:
                    # 用 wait_for 卡住整体截止时间：aiohttp 自身的 receive 超时会被服务端 ping 重置
                    msg = await asyncio.wait_for(ws.receive(), timeout=max(0.01, remaining))
                except asyncio.TimeoutError:
                    self.idle_reconnects += 1
                    log.warning("%.0fs 内没有收到任何行情数据，主动断开重连", self.idle_timeout)
                    return
                if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                    last_data = self.last_msg = time.monotonic()
                    self._dispatch(msg.data if msg.type == aiohttp.WSMsgType.TEXT else msg.data.decode())
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                                  aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    log.warning("行情流被关闭：%s %s", msg.type.name, ws.close_code)
                    return
                if time.monotonic() - started > WS_MAX_AGE:
                    log.info("连接已接近 24 小时上限，主动重连（缺口由 REST 补齐）")
                    return

    async def run(self) -> None:
        backoff = 1.0
        while True:
            started = time.monotonic()
            self.state = "connecting"
            try:
                await self._session_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 网络错误统一重连
                log.warning("行情流断开：%r", e)
            if time.monotonic() - started > 60:
                backoff = 1.0  # 稳定运行过一段时间，退避复位
            self.state = "reconnecting"
            wait = min(60.0, backoff) * (0.8 + 0.4 * random.random())
            log.info("%.1fs 后重连", wait)
            await asyncio.sleep(wait)
            backoff = min(60.0, backoff * 2)
