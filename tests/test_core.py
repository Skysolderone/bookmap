import asyncio
import csv
import io
import pickle
import tempfile
import unittest
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from decimal import Decimal

from fp_binance import DAY_MS, MarketStream, RestRangeError, SymbolFeed, resolve_proxy
from fp_history import iter_zip_rows, load_zip_book
from fp_core import (SLOT_MS, Book, SymbolSpec, bucket_choices, bucket_floor, build_bars,
                     default_bucket_ticks, is_taker_buy, nice_number, to_scaled)

ETH = SymbolSpec("ETHUSDT", "0.01", "0.001")
BTC = SymbolSpec("BTCUSDT", "0.10", "0.001")


def tr(a, T, p, q, m):
    return {"a": a, "T": T, "p": p, "q": q, "m": m}


class TestSide(unittest.TestCase):
    def test_m_flag(self):
        self.assertFalse(is_taker_buy(True))   # 买方为挂单方 -> 主动卖
        self.assertTrue(is_taker_buy(False))   # 卖方为挂单方 -> 主动买

    def test_book_side(self):
        b = Book(ETH)
        b.apply_raw(tr(1, 0, "2000.00", "1.5", False))
        b.apply_raw(tr(2, 1, "2000.00", "0.25", True))
        s = b.slots[0]
        self.assertEqual((s.buy, s.sell), (1500, 250))
        self.assertEqual(s.cells[200000], [1500, 250, 1, 1])  # 买量, 卖量, 买笔数, 卖笔数


class TestScaling(unittest.TestCase):
    def test_to_scaled(self):
        self.assertEqual(to_scaled("2714.32", 2), 271432)
        self.assertEqual(to_scaled("86109.80", 1), 861098)
        self.assertEqual(to_scaled("3.5", 3), 3500)
        self.assertEqual(to_scaled("7", 0), 7)
        self.assertEqual(to_scaled("0.001", 3), 1)
        with self.assertRaises(ValueError):
            to_scaled("1.2345", 2)

    def test_spec(self):
        self.assertEqual((BTC.price_dec, BTC.tick_units), (1, 1))
        self.assertEqual(BTC.price_ticks("86109.80"), 861098)
        odd = SymbolSpec("X", "0.05", "1")
        self.assertEqual(odd.price_ticks("1.25"), 25)
        self.assertEqual(odd.price_ticks("1.27"), 25)  # 不在 tick 上时向下取整
        self.assertEqual(odd.qty_units("12"), 12)


class TestBucket(unittest.TestCase):
    def test_floor(self):
        # ETH 1 USDT = 100 tick
        self.assertEqual(bucket_floor(ETH.price_ticks("2714.99"), 100), 271400)
        self.assertEqual(bucket_floor(ETH.price_ticks("2715.00"), 100), 271500)
        self.assertEqual(bucket_floor(ETH.price_ticks("2714.00"), 100), 271400)
        self.assertEqual(bucket_floor(-1, 100), -100)
        # 浮点下 0.1+0.2 之类的误差不会出现：0.30 USDT 分桶
        x = SymbolSpec("X", "0.0001", "1")
        self.assertEqual(bucket_floor(x.price_ticks("0.3000"), 1000), 3000)
        self.assertEqual(bucket_floor(x.price_ticks("0.2999"), 1000), 2000)

    def test_nice(self):
        self.assertEqual(nice_number(25.8), Decimal(20))
        self.assertEqual(nice_number(36), Decimal(50))
        self.assertEqual(nice_number(0.0042), Decimal("0.005"))
        self.assertEqual(nice_number(1.4), Decimal(1))

    def test_defaults(self):
        self.assertEqual(default_bucket_ticks(ETH, 2714.0), 100)       # 1 USDT
        self.assertEqual(default_bucket_ticks(BTC, 86000.0), 200)      # 25.8 -> 20 USDT = 200 tick
        sol = SymbolSpec("SOLUSDT", "0.0100", "1")
        self.assertEqual(default_bucket_ticks(sol, 150.0), 5)          # 0.045 -> 0.05
        tiny = SymbolSpec("T", "0.10", "1")
        self.assertEqual(default_bucket_ticks(tiny, 10.0), 1)          # 不小于 1 tick

    def test_choices(self):
        ch = bucket_choices(ETH, 2714.0)
        self.assertIn(1, ch)
        self.assertIn(100, ch)
        self.assertEqual(ch, sorted(ch))
        self.assertTrue(all(c >= 1 for c in ch))


class TestBars(unittest.TestCase):
    def _book(self):
        b = Book(ETH)
        t0 = 1_700_000_040_000  # 1 分钟整点
        self.assertEqual(t0 % 60000, 0)
        trades = [
            tr(1, t0, "2000.10", "1.000", False),          # buy 1 @2000
            tr(2, t0 + 10, "2000.90", "2.000", True),      # sell 2 @2000
            tr(3, t0 + 29_999, "2001.00", "3.000", False),  # buy 3 @2001
            tr(4, t0 + 30_000, "1999.99", "4.000", True),   # sell 4 @1999（第二个 30s 槽，同一根 1m）
            tr(5, t0 + 59_999, "2001.50", "0.500", False),  # buy 0.5 @2001
            tr(6, t0 + 60_000, "2002.00", "1.000", True),   # 下一根 1m
        ]
        for d in trades:
            b.apply_raw(d)
        return b, t0

    def test_boundaries(self):
        b, t0 = self._book()
        bars = build_bars(b, 2, 100)
        self.assertEqual([x["t"] for x in bars], [t0, t0 + 60000])
        self.assertEqual(bars[0]["n"], 5)
        self.assertEqual(bars[1]["n"], 1)
        bars30 = build_bars(b, 1, 100)
        self.assertEqual([x["t"] for x in bars30], [t0, t0 + 30000, t0 + 60000])
        self.assertEqual(bars30[0]["n"], 3)
        # 5m 周期：t0 不在 5 分钟边界上时归入所在的 5 分钟 K 线
        bars5 = build_bars(b, 10, 100)
        self.assertEqual(len(bars5), 1 if (t0 // 300000) == ((t0 + 60000) // 300000) else 2)
        self.assertEqual(bars5[0]["t"] % 300000, 0)

    def test_delta_poc(self):
        b, t0 = self._book()
        bar = build_bars(b, 2, 100)[0]
        self.assertEqual(bar["buy"], 4500)
        self.assertEqual(bar["sell"], 6000)
        self.assertEqual(bar["delta"], -1500)
        self.assertEqual(bar["v"], 10500)
        self.assertEqual((bar["o"], bar["h"], bar["l"], bar["c"]), (200010, 200150, 199999, 200150))
        cells = {c[0]: (c[1], c[2]) for c in bar["cells"]}
        self.assertEqual({c[0]: (c[3], c[4]) for c in bar["cells"]}, {200100: (2, 0), 200000: (1, 1), 199900: (0, 1)})
        self.assertEqual(cells, {200100: (3500, 0), 200000: (1000, 2000), 199900: (0, 4000)})
        self.assertEqual([c[0] for c in bar["cells"]], [200100, 200000, 199900])  # 高到低
        self.assertEqual(bar["poc"], 199900)   # 4.0 最大（2001 档 3.5，2000 档 3.0）
        # 槽内累计 delta 路径：+1, -1, +2 | -2, -1.5  -> max 2.0, min -2.0
        self.assertEqual((bar["dmax"], bar["dmin"]), (2000, -2000))
        bars = build_bars(b, 2, 100)
        self.assertEqual(bars[1]["cvd"], -1500 - 1000)

    def test_poc_tie_lower_price(self):
        b = Book(ETH)
        b.apply_raw(tr(1, 0, "10.00", "1", False))
        b.apply_raw(tr(2, 1, "11.00", "1", True))
        self.assertEqual(build_bars(b, 2, 100)[0]["poc"], 1000)

    def test_bucket_change_consistent(self):
        b, _ = self._book()
        fine = build_bars(b, 2, 1)[0]
        coarse = build_bars(b, 2, 500)[0]
        self.assertEqual(sum(c[1] for c in fine["cells"]), sum(c[1] for c in coarse["cells"]))
        self.assertEqual(coarse["cells"], [[200000, 4500, 2000, 3, 1], [199500, 0, 4000, 0, 1]])

    def test_bar_ids_subset(self):
        b, t0 = self._book()
        sub = build_bars(b, 2, 100, bar_ids={(t0 + 60000) // SLOT_MS // 2})
        self.assertEqual([x["t"] for x in sub], [t0 + 60000])

    def test_trim(self):
        b, t0 = self._book()
        b.trim(t0 + 30000)
        self.assertEqual(sorted(b.slots), [(t0 + 30000) // SLOT_MS, (t0 + 60000) // SLOT_MS])


class TestFreezeMerge(unittest.TestCase):
    def _book(self, n=200, start_id=1, t0=1_700_000_040_000):
        b = Book(ETH)
        for i in range(n):
            b.apply_raw(tr(start_id + i, t0 + i * 750, f"{2000 + (i * 7) % 13}.{i % 100:02d}", f"0.{i % 9 + 1}", i % 3 == 0))
        return b

    def test_freeze_same_result_and_thaw_on_add(self):
        b = self._book()
        before = build_bars(b, 2, 37)
        self.assertGreater(b.freeze_before(1 << 62), 0)
        self.assertTrue(all(s.cells is None for s in b.slots.values()))
        self.assertEqual(build_bars(b, 2, 37), before)
        # 冻结的槽再收到成交（如补缺）会自动解冻
        last_sid = max(b.slots)
        b.apply_raw(tr(10_000, last_sid * SLOT_MS + 1, "2001.00", "1", False))
        self.assertIsNotNone(b.slots[last_sid].cells)

    def test_pickle_roundtrip(self):
        b = self._book()
        b2 = pickle.loads(pickle.dumps(b))
        self.assertEqual(build_bars(b2, 2, 100), build_bars(b, 2, 100))
        self.assertEqual(b2.spec.price_ticks("1.23"), 123)

    def test_merge_newer(self):
        t0 = 1_700_000_040_000
        whole = self._book(400)
        first = self._book(200)
        second = Book(ETH)
        for i in range(200, 400):
            second.apply_raw(tr(1 + i, t0 + i * 750, f"{2000 + (i * 7) % 13}.{i % 100:02d}", f"0.{i % 9 + 1}", i % 3 == 0))
        # 两段在 30s 槽边界处切开（按 UTC 天切分时总成立）
        self.assertNotEqual((t0 + 199 * 750) // SLOT_MS, (t0 + 200 * 750) // SLOT_MS)
        self.assertEqual(first.merge_newer(second), 0)
        self.assertEqual(build_bars(first, 2, 100), build_bars(whole, 2, 100))
        gap = Book(ETH)
        gap.apply_raw(tr(1000, t0 + 10**7, "2000", "1", False))
        expect = 1000 - first.last_id - 1
        self.assertEqual(first.merge_newer(gap), expect)
        with self.assertRaises(ValueError):
            first.merge_newer(gap)  # 重叠

    def test_cvd_base_and_limit(self):
        b = self._book(400)
        full = build_bars(b, 2, 100)
        tail = build_bars(b, 2, 100, limit=2, cvd_base=b.delta_before(full[-2]["t"]))
        self.assertEqual([x["t"] for x in tail], [x["t"] for x in full[-2:]])
        self.assertEqual([x["cvd"] for x in tail], [x["cvd"] for x in full[-2:]])
        mid = build_bars(b, 2, 100, start_ms=full[1]["t"], end_ms=full[3]["t"])
        self.assertEqual([x["t"] for x in mid], [full[1]["t"], full[2]["t"]])


def make_zip(path: Path, rows, header=True):
    buf = io.StringIO()
    w = csv.writer(buf)
    if header:
        w.writerow(["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker"])
    for d in rows:
        w.writerow([d["a"], d["p"], d["q"], d["a"] * 2, d["a"] * 2 + 1, d["T"], "true" if d["m"] else "false"])
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(path.stem + ".csv", buf.getvalue())


class TestZip(unittest.TestCase):
    def test_header_and_no_header(self):
        rows = mk(50)
        with tempfile.TemporaryDirectory() as d:
            for header in (True, False):
                p = Path(d) / f"X-aggTrades-{header}.zip"
                make_zip(p, rows, header)
                got = list(iter_zip_rows(p))
                self.assertEqual(len(got), 50)
                self.assertEqual(got[0], (str(rows[0]["a"]), "2000.00", "0.001", str(rows[0]["T"]), "true"))
                b = load_zip_book(str(p), ETH)
                ref = Book(ETH)
                for r in rows:
                    ref.apply_raw(r)
                self.assertEqual(build_bars(b, 2, 100), build_bars(ref, 2, 100))
                self.assertEqual((b.first_id, b.last_id), (rows[0]["a"], rows[-1]["a"]))


class FakeRest:
    """模拟 /fapi/v1/aggTrades：trades 为按 ID 连续的完整成交序列。"""

    def __init__(self, trades, page=1000, min_id=None):
        self.trades = trades
        self.page = page
        self.min_id = min_id  # 早于此 ID 的 fromId 返回 -4166（超出 2 天）
        self.calls = []

    async def __call__(self, symbol, *, from_id=None, start_time=None, end_time=None, limit=1000):
        self.calls.append((from_id, start_time))
        assert not (from_id is not None and start_time is not None)
        if from_id is not None and self.min_id is not None and from_id < self.min_id:
            raise RestRangeError("-4166")
        if from_id is not None:
            rows = [t for t in self.trades if t["a"] >= from_id]
        else:
            rows = [t for t in self.trades if t["T"] >= start_time]
        return rows[: min(limit, self.page)]


def mk(n, t0=1_700_000_000_000, start_id=100):
    return [tr(start_id + i, t0 + i * 100, "2000.00", "0.001", i % 2 == 0) for i in range(n)]


class TestFeed(unittest.TestCase):
    def _run(self, feed, ws_msgs):
        async def go():
            for d in ws_msgs:
                feed.queue.put_nowait(d)
            task = asyncio.create_task(feed.run())
            for _ in range(200):
                await asyncio.sleep(0)
                if feed.queue.empty() and feed.state == "live":
                    break
            await asyncio.sleep(0.01)
            task.cancel()
        asyncio.run(go())

    def _feed(self, all_trades, page=1000, backfill_ms=10_000, now=None):
        rest = FakeRest(all_trades, page)
        book = Book(ETH)
        now = now or all_trades[-1]["T"] + 50
        feed = SymbolFeed(book, rest, backfill_ms=backfill_ms, retention_ms=6 * 3600_000, now_ms=lambda: now)
        return feed, rest, book

    def test_backfill_then_live_dedup(self):
        trades = mk(3000)
        # REST 只能看到前 2500 条（之后的还在 WS 里），WS 从 2400 开始，重叠 100 条
        feed, rest, book = self._feed(trades[:2500], page=1000, backfill_ms=10**9)
        self._run(feed, trades[2400:])
        self.assertEqual(book.trades, 3000)
        self.assertEqual(book.last_id, trades[-1]["a"])
        self.assertEqual(sum(s.n for s in book.slots.values()), 3000)
        self.assertIsNone(rest.calls[0][0])        # 第一页按 startTime
        self.assertTrue(all(c[1] is None for c in rest.calls[1:]))  # 之后用 fromId

    def test_gap_fill(self):
        trades = mk(500)
        feed, rest, book = self._feed(trades, page=100, backfill_ms=0)
        # WS 收到 0..99，然后断线，重连后从 400 开始
        self._run(feed, trades[:100] + trades[400:])
        self.assertEqual(book.trades, 500)
        self.assertEqual(feed.gap_fills, 1)
        self.assertEqual(feed.gap_trades, 300)  # 缺口 ID 200..499
        self.assertEqual(feed.lost, 0)
        self.assertEqual([c[0] for c in rest.calls], [200, 300, 400])  # 用 fromId 翻页
        self.assertEqual(book.last_id, trades[-1]["a"])

    def test_duplicates_ignored(self):
        trades = mk(10)
        feed, rest, book = self._feed(trades, backfill_ms=0)
        self._run(feed, trades[:5] + trades[3:8] + trades[8:])
        self.assertEqual(book.trades, 10)
        self.assertEqual(feed.gap_fills, 0)

    def test_unrecoverable_gap(self):
        trades = mk(20)
        rest_view = trades[:5] + trades[10:]  # 交易所也查不到 5..9
        feed, rest, book = self._feed(rest_view, backfill_ms=0)
        self._run(feed, trades[:5] + trades[12:])
        self.assertEqual(feed.lost, 5)
        self.assertEqual(book.trades, 15)


class TestCsvSaver(unittest.TestCase):
    def test_resume_and_truncate_partial_line(self):
        import tempfile
        from pathlib import Path
        from footprint import CsvSaver
        T = 1_791_200_000_000
        with tempfile.TemporaryDirectory() as d:
            s = CsvSaver(Path(d))
            for a in (1, 2):
                s.write("X", tr(a, T, "1.0", "2", a == 1))
            s.close()
            f = next(Path(d).rglob("*.csv"))
            f.write_bytes(f.read_bytes() + b"3,1.0")  # 模拟被强杀留下的半行
            s2 = CsvSaver(Path(d))
            for a in (2, 3, 4):  # 2 是重启回补带来的重复
                s2.write("X", tr(a, T, "1.5", "1", False))
            s2.close()
            rows = f.read_text().splitlines()
            self.assertEqual(rows[0], "a,p,q,T,m")
            self.assertEqual([r.split(",")[0] for r in rows[1:]], ["1", "2", "3", "4"])
            self.assertEqual(rows[1], f"1,1.0,2,{T},true")


class TestHistoryFlow(unittest.TestCase):
    """--backfill-days：历史包 + REST 接续，全部按 ID 衔接。"""
    N = 300  # 每天成交数

    def setUp(self):
        self.today0 = 1_791_158_400_000  # 某个 UTC 零点
        self.assertEqual(self.today0 % DAY_MS, 0)
        self.now = self.today0 + 10 * 3600_000
        # 第 -3、-2、-1 天和今天各 N 笔，ID 连续
        self.days = {}
        a = 1000
        for k in (-3, -2, -1, 0):
            day0 = self.today0 + k * DAY_MS
            rows = [tr(a + i, day0 + i * 1000, "2000.00", "0.001", i % 2 == 0) for i in range(self.N)]
            self.days[k] = rows
            a += self.N
        self.all = [r for k in (-3, -2, -1, 0) for r in self.days[k]]

    def _day_book(self, k):
        b = Book(ETH)
        for r in self.days[k]:
            b.apply_raw(r)
        b.dirty.clear()
        b.freeze_before(1 << 62)
        return b

    def _run(self, days, available, rest_rows, min_id=None):
        rest = FakeRest(rest_rows, page=1000, min_id=min_id)
        book = Book(ETH)
        loaded = []

        async def load_day(d):
            k = (datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000 - self.today0) // DAY_MS
            loaded.append(int(k))
            return self._day_book(int(k)) if int(k) in available else None

        feed = SymbolFeed(book, rest, backfill_ms=0, retention_ms=10 * DAY_MS, backfill_days=days,
                          load_day=load_day, now_ms=lambda: self.now)

        async def go():
            t = asyncio.create_task(feed.run())
            for _ in range(500):
                await asyncio.sleep(0)
                if feed.state == "live":
                    break
            t.cancel()
        asyncio.run(go())
        return feed, rest, book, loaded

    def test_all_zips_then_rest_by_id(self):
        feed, rest, book, loaded = self._run(2, {-2, -1}, self.all)
        self.assertEqual(loaded, [-2, -1])
        self.assertEqual(book.trades, 3 * self.N)
        self.assertEqual((book.first_id, book.last_id), (self.days[-2][0]["a"], self.all[-1]["a"]))
        self.assertEqual(feed.lost, 0)
        self.assertTrue(all(c[1] is None for c in rest.calls))  # 今天全部用 fromId 翻页
        self.assertEqual(rest.calls[0][0], self.days[-1][-1]["a"] + 1)

    def test_yesterday_zip_missing_falls_back_to_rest(self):
        feed, rest, book, loaded = self._run(2, {-2}, self.all)
        self.assertEqual(loaded, [-2, -1])
        self.assertEqual(book.trades, 3 * self.N)
        self.assertEqual(feed.lost, 0)
        self.assertEqual(rest.calls[0][0], self.days[-2][-1]["a"] + 1)

    def test_no_zip_rest_from_day_start(self):
        feed, rest, book, loaded = self._run(1, set(), self.all)
        self.assertEqual(rest.calls[0], (None, self.today0 - DAY_MS))  # 昨天零点按 startTime
        self.assertEqual(book.trades, 2 * self.N)

    def test_old_day_missing_is_lost(self):
        feed, rest, book, loaded = self._run(3, {-3, -1}, self.all)  # -2 天超出 REST 范围且无包
        self.assertEqual(loaded, [-3, -2, -1])
        self.assertEqual(feed.lost, self.N)
        self.assertEqual(book.trades, 3 * self.N)

    def test_catch_up_out_of_range_rebackfills_by_time(self):
        feed, rest, book, loaded = self._run(1, {-1}, self.all, min_id=self.days[0][0]["a"] + 10)
        self.assertEqual(rest.calls[0][0], self.days[-1][-1]["a"] + 1)  # 先尝试 fromId
        self.assertIsNotNone(rest.calls[1][1])                        # -4166 后改按时间
        self.assertEqual(book.last_id, self.all[-1]["a"])


class TestIdleTimeout(unittest.TestCase):
    def test_reconnect_when_only_pings(self):
        from aiohttp import web

        async def go():
            async def handler(req):
                ws = web.WebSocketResponse(autoping=True)
                await ws.prepare(req)
                try:
                    while not ws.closed:  # 只发 ping，不发任何数据
                        await ws.ping()
                        await asyncio.sleep(0.1)
                except ConnectionError:
                    pass
                return ws

            app = web.Application()
            app.router.add_get("/ws", handler)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            port = site._server.sockets[0].getsockname()[1]
            import aiohttp
            async with aiohttp.ClientSession() as s:
                ms = MarketStream(s, None, {}, idle_timeout=0.4, url=f"http://127.0.0.1:{port}/ws")
                t = asyncio.create_task(ms.run())
                await asyncio.sleep(2.2)  # 0.4s 超时 + ~1s 退避 + 0.4s
                t.cancel()
            await runner.cleanup()
            return ms

        ms = asyncio.run(go())
        self.assertGreaterEqual(ms.idle_reconnects, 2)
        self.assertGreaterEqual(ms.connects, 2)


class TestProxy(unittest.TestCase):
    def test_resolve(self):
        import os
        old = {k: os.environ.pop(k, None) for k in ("HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy")}
        try:
            self.assertIsNone(resolve_proxy(None))
            os.environ["ALL_PROXY"] = "socks5://127.0.0.1:1080"
            self.assertEqual(resolve_proxy(None), "socks5://127.0.0.1:1080")
            os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7890"
            self.assertEqual(resolve_proxy(None), "http://127.0.0.1:7890")
            self.assertEqual(resolve_proxy("http://x:1"), "http://x:1")
        finally:
            for k, v in old.items():
                os.environ.pop(k, None)
                if v is not None:
                    os.environ[k] = v


if __name__ == "__main__":
    unittest.main()
