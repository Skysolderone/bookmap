#!/usr/bin/env python3
"""币安 U 本位永续合约 足迹图（Footprint）本地工具。

python footprint.py --symbols ETHUSDT,BTCUSDT --interval 1m --backfill 30 --port 8765
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import WSMsgType, web

from concurrent.futures import ProcessPoolExecutor

from fp_binance import (DAY_MS, REST_HISTORY_MS, BinanceRest, MarketStream, SymbolFeed, load_specs,
                        make_session, resolve_proxy)
from fp_core import (INTERVALS, SLOT_MS, Book, bucket_choices, build_bars, default_bucket_ticks)
from fp_history import VisionStore, load_zip_book

log = logging.getLogger("footprint")
HERE = Path(__file__).resolve().parent
PUSH_INTERVAL = 0.25
SNAPSHOT_BARS = 600      # 订阅/加载更早数据时每次发送的 K 线数
RESYNC_BARS = 300        # 一次变动的 K 线超过这个数（如加载完一天历史）时改发整份快照


class CsvSaver:
    """按 UTC 日期把原始 aggTrade 追加到 <dir>/<SYMBOL>/<SYMBOL>-aggTrades-YYYY-MM-DD.csv。"""

    FIELDS = ["a", "p", "q", "T", "m"]

    def __init__(self, root: Path):
        self.root = root
        self.files: dict[str, tuple[str, object, object]] = {}
        self.last_saved: dict[str, int] = {}

    def _path(self, sym: str, day: str) -> Path:
        return self.root / sym / f"{sym}-aggTrades-{day}.csv"

    def _open(self, sym: str, day: str):
        p = self._path(sym, day)
        p.parent.mkdir(parents=True, exist_ok=True)
        new = not p.exists() or p.stat().st_size == 0
        if not new:
            with p.open("r+b") as f:
                size = f.seek(0, 2)
                f.seek(max(0, size - 4096))
                tail = f.read()
                # 上次被强杀时可能留下半行：截断到最后一个换行
                if not tail.endswith(b"\n"):
                    cut = tail.rfind(b"\n")
                    f.truncate(size - len(tail) + cut + 1 if cut >= 0 else 0)
                    tail = tail[: cut + 1] if cut >= 0 else b""
            new = p.stat().st_size == 0
            if sym not in self.last_saved:
                # 重启时读取已有文件的最后一个 ID，避免回补重复写入
                lines = [ln for ln in tail.decode(errors="ignore").splitlines() if ln.strip()]
                if lines and lines[-1][0].isdigit():
                    self.last_saved[sym] = int(lines[-1].split(",")[0])
        f = p.open("a", newline="")
        w = csv.writer(f)
        if new:
            w.writerow(self.FIELDS)
        return f, w

    def write(self, sym: str, d: dict) -> None:
        if d["a"] <= self.last_saved.get(sym, -1):
            return
        day = datetime.fromtimestamp(d["T"] / 1000, timezone.utc).strftime("%Y-%m-%d")
        cur = self.files.get(sym)
        if cur is None or cur[0] != day:
            if cur:
                cur[1].close()
            f, w = self._open(sym, day)
            cur = self.files[sym] = (day, f, w)
            if d["a"] <= self.last_saved.get(sym, -1):
                return
        cur[2].writerow([d["a"], d["p"], d["q"], d["T"], "true" if d["m"] else "false"])
        self.last_saved[sym] = d["a"]

    def flush(self) -> None:
        for _, f, _ in self.files.values():
            f.flush()

    def close(self) -> None:
        for _, f, _ in self.files.values():
            f.close()
        self.files.clear()


class App:
    def __init__(self, args):
        self.args = args
        self.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        self.retention_ms = int(args.retention * 3600 * 1000)
        self.backfill_days = max(0, int(args.backfill_days))
        if self.backfill_days:
            # 保留时长至少覆盖：N 天历史 + 今天已过去的部分 + 1 小时余量
            now = int(time.time() * 1000)
            self.retention_ms = max(self.retention_ms, self.backfill_days * DAY_MS + now % DAY_MS + 3600_000)
        self.backfill_ms = min(int(args.backfill * 60 * 1000), self.retention_ms, REST_HISTORY_MS)
        self.proxy = resolve_proxy(args.proxy)
        self.feeds: dict[str, SymbolFeed] = {}
        self.books: dict[str, Book] = {}
        self.defaults: dict[str, dict] = {}
        self.clients: dict[web.WebSocketResponse, dict] = {}
        self.saver = CsvSaver(Path(args.save)) if args.save else None
        self.stream: MarketStream | None = None

    # ---------- 页面推送 ----------
    def meta(self) -> dict:
        return {
            "symbols": self.symbols,
            "intervals": list(INTERVALS),
            "defaultInterval": self.args.interval,
            "retentionMs": self.retention_ms,
            "specs": {s: {**self.books[s].spec.meta(), **self.defaults[s]} for s in self.symbols},
        }

    def status(self) -> dict:
        now = time.time() * 1000
        st = {}
        for s, f in self.feeds.items():
            b = f.book
            st[s] = {
                "state": f.state, "progress": f.progress, "trades": b.trades,
                "lagMs": max(0, int(now - b.last_T)) if b.last_T else None,  # 本机时钟略快时不显示负数
                "gapFills": f.gap_fills, "gapTrades": f.gap_trades, "lost": f.lost,
                "queue": f.queue.qsize(),
            }
        return {"type": "status", "ws": self.stream.state if self.stream else "init",
                "connects": self.stream.connects if self.stream else 0,
                "idleReconnects": self.stream.idle_reconnects if self.stream else 0, "feeds": st}

    def _sub_params(self, msg: dict) -> dict:
        sym = msg.get("symbol") if msg.get("symbol") in self.books else self.symbols[0]
        iv = msg.get("interval") if msg.get("interval") in INTERVALS else self.args.interval
        bucket = msg.get("bucket")
        if not isinstance(bucket, int) or bucket < 1 or bucket > 10**9:
            bucket = self.defaults[sym]["defaultBucket"]
        return {"symbol": sym, "interval": iv, "k": INTERVALS[iv], "bucket": bucket}

    def _window(self, book: Book, sub: dict, before: int | None = None) -> dict:
        """最近（或 before 之前）的 SNAPSHOT_BARS 根 K 线，cvd 为保留数据范围内的绝对累计值。"""
        bars = build_bars(book, sub["k"], sub["bucket"], end_ms=before, limit=SNAPSHOT_BARS)
        if bars:
            base = book.delta_before(bars[0]["t"])
            for b in bars:
                base += b["delta"]
                b["cvd"] = base
        more = bool(bars) and min(book.slots) * SLOT_MS < bars[0]["t"]  # 是否还有更早的数据
        return {"symbol": sub["symbol"], "interval": sub["interval"], "bucket": sub["bucket"],
                "bars": bars, "more": more}

    async def _send_snapshot(self, ws, sub: dict) -> None:
        w = self._window(self.books[sub["symbol"]], sub)
        sub["from_t"] = w["bars"][0]["t"] if w["bars"] else 0
        sub["last_snap"] = time.monotonic()
        sub["resync"] = False
        await ws.send_str(json.dumps({"type": "snapshot", **w}, separators=(",", ":")))

    async def ws_handler(self, req: web.Request):
        ws = web.WebSocketResponse(heartbeat=30, max_msg_size=1 << 20)
        await ws.prepare(req)
        await ws.send_json({"type": "meta", **self.meta()})
        await ws.send_json(self.status())
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    m = json.loads(msg.data)
                except ValueError:
                    continue
                if m.get("type") == "subscribe":
                    sub = self._sub_params(m)
                    self.clients[ws] = sub
                    await self._send_snapshot(ws, sub)
                elif m.get("type") == "history" and ws in self.clients and isinstance(m.get("before"), int):
                    # 页面向左拖到已加载数据的开头时，按需加载更早的 K 线
                    sub = self.clients[ws]
                    if (m.get("symbol"), m.get("interval"), m.get("bucket")) != (sub["symbol"], sub["interval"], sub["bucket"]):
                        continue  # 过期请求（期间已切换订阅）
                    w = self._window(self.books[sub["symbol"]], sub, before=m["before"])
                    if w["bars"]:
                        sub["from_t"] = min(sub["from_t"], w["bars"][0]["t"])
                    await ws.send_str(json.dumps({"type": "history", **w}, separators=(",", ":")))
        finally:
            self.clients.pop(ws, None)
        return ws

    async def pusher(self):
        last_status = 0.0
        while True:
            await asyncio.sleep(PUSH_INTERVAL)
            for sym, book in self.books.items():
                dirty = book.pop_dirty()
                if not dirty:
                    continue
                for ws, sub in list(self.clients.items()):
                    if sub["symbol"] != sym or ws.closed or sub["resync"]:
                        continue
                    k = sub["k"]
                    bar_ids = {s // k for s in dirty}
                    # 变动落在页面已加载范围之前（会改变 CVD 起点）或一次变动太多：改发整份快照
                    if len(bar_ids) > RESYNC_BARS or min(bar_ids) * k * SLOT_MS < sub["from_t"]:
                        sub["resync"] = True
                        continue
                    bars = build_bars(book, k, sub["bucket"], bar_ids=bar_ids)
                    try:
                        await ws.send_str(json.dumps({"type": "bars", "symbol": sym, "interval": sub["interval"],
                                                      "bucket": sub["bucket"], "bars": bars}, separators=(",", ":")))
                    except ConnectionError:
                        pass
            for ws, sub in list(self.clients.items()):
                if sub["resync"] and not ws.closed and time.monotonic() - sub["last_snap"] >= 1.0:
                    try:
                        await self._send_snapshot(ws, sub)
                    except ConnectionError:
                        pass
            if time.time() - last_status >= 1:
                last_status = time.time()
                st = self.status()
                for ws in list(self.clients):
                    if not ws.closed:
                        try:
                            await ws.send_json(st)
                        except ConnectionError:
                            pass
                if self.saver:
                    self.saver.flush()

    async def trimmer(self):
        while True:
            await asyncio.sleep(30)
            now = int(time.time() * 1000)
            for b in self.books.values():
                b.trim(now - self.retention_ms)
                b.freeze_before(now - 120_000)  # 两分钟前的槽已收盘，压缩存储

    async def api_bars(self, req: web.Request):
        """供对账脚本使用：/api/bars?symbol=ETHUSDT&interval=1m&bucket=100"""
        sub = self._sub_params({"symbol": req.query.get("symbol"), "interval": req.query.get("interval"),
                                "bucket": int(req.query["bucket"]) if req.query.get("bucket", "").isdigit() else None})
        limit = int(req.query.get("limit", "1500"))
        bars = build_bars(self.books[sub["symbol"]], sub["k"], sub["bucket"], limit=limit)
        return web.json_response({**{k: sub[k] for k in ("symbol", "interval", "bucket")},
                                  "spec": self.books[sub["symbol"]].spec.meta(), "bars": bars})

    async def index(self, req):
        return web.FileResponse(HERE / "static" / "index.html", headers={"Cache-Control": "no-store"})

    @staticmethod
    def _day_loader(store: VisionStore, pool: ProcessPoolExecutor, spec):
        async def load(day):
            path = await store.fetch(spec.symbol, day)
            if path is None:
                return None
            return await asyncio.get_running_loop().run_in_executor(pool, load_zip_book, str(path), spec)
        return load

    # ---------- 启动 ----------
    async def run(self):
        # kill / SIGTERM 时走正常退出流程，保证 CSV 刷盘
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)
        session, req_proxy = make_session(self.proxy)
        if self.proxy:
            log.info("使用代理：%s", self.proxy)
        rest = BinanceRest(session, req_proxy)
        try:
            info = await rest.exchange_info()
        except Exception as e:
            await session.close()
            raise SystemExit(f"无法访问币安 REST（{e!r}）。请检查网络或代理设置（--proxy / HTTPS_PROXY / ALL_PROXY）。")
        specs = load_specs(info, self.symbols)
        pool = None
        store = None
        if self.backfill_days:
            # 历史包在子进程里解析（CPU 密集，不阻塞事件循环；多个品种并行），进程数不超过 4
            pool = ProcessPoolExecutor(max_workers=min(4, len(self.symbols)))
            store = VisionStore(session, req_proxy, Path(self.args.cache_dir).expanduser())
            log.info("历史日包：%d 天，缓存目录 %s，内存保留 %.1f 小时", self.backfill_days,
                     store.cache_dir, self.retention_ms / 3600_000)
        for sym, spec in specs.items():
            price = await rest.ticker_price(sym)
            self.defaults[sym] = {"defaultBucket": default_bucket_ticks(spec, price),
                                  "bucketChoices": bucket_choices(spec, price)}
            book = self.books[sym] = Book(spec)
            on_trade = (lambda d, s=sym: self.saver.write(s, d)) if self.saver else None
            self.feeds[sym] = SymbolFeed(book, rest.agg_trades, backfill_ms=self.backfill_ms,
                                         retention_ms=self.retention_ms, on_trade=on_trade,
                                         backfill_days=self.backfill_days,
                                         load_day=self._day_loader(store, pool, spec) if pool else None)

        app = web.Application()
        app.router.add_get("/", self.index)
        app.router.add_get("/ws", self.ws_handler)
        app.router.add_get("/api/bars", self.api_bars)
        app.router.add_get("/api/meta", lambda r: web.json_response(self.meta()))
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", self.args.port).start()
        print(f"\n  足迹图已启动：http://127.0.0.1:{self.args.port}/\n", flush=True)

        # 先连 WS（消息进队列缓冲），再回补历史，二者通过 ID 衔接
        self.stream = MarketStream(session, req_proxy, self.feeds)
        tasks = [asyncio.create_task(self.stream.run(), name="stream"),
                 asyncio.create_task(self.pusher(), name="pusher"),
                 asyncio.create_task(self.trimmer(), name="trimmer")]
        tasks += [asyncio.create_task(f.run(), name=f"feed-{s}") for s, f in self.feeds.items()]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in done:
                t.result()
        finally:
            for t in tasks:
                t.cancel()
            await runner.cleanup()
            await session.close()
            if pool:
                pool.shutdown(wait=False, cancel_futures=True)
            if self.saver:
                self.saver.close()


def main():
    p = argparse.ArgumentParser(description="币安 U 本位永续合约足迹图（只用公开行情接口）")
    p.add_argument("--symbols", default="ETHUSDT", help="逗号分隔，如 ETHUSDT,BTCUSDT")
    p.add_argument("--interval", default="1m", choices=list(INTERVALS), help="页面默认周期")
    p.add_argument("--backfill", type=float, default=30, help="启动回补分钟数（默认 30，最大不超过保留时长）")
    p.add_argument("--backfill-days", type=int, default=0,
                   help="额外加载今天（UTC）之前多少天的历史：从 data.binance.vision 下载每日 aggTrades 包（默认 0）")
    p.add_argument("--cache-dir", default=str(HERE / "data" / "binance-vision"), help="历史日包缓存目录")
    p.add_argument("--retention", type=float, default=6, help="内存保留小时数（默认 6；启用 --backfill-days 时自动延长）")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--proxy", default=None,
                   help="代理，如 http://127.0.0.1:7890 或 socks5://127.0.0.1:7891；默认读 HTTPS_PROXY / ALL_PROXY")
    p.add_argument("--save", default=None, metavar="DIR", help="把原始 aggTrade 按天追加保存为 CSV（默认关闭）")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(App(args).run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


if __name__ == "__main__":
    main()
