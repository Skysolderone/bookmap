#!/usr/bin/env python3
"""对账：把足迹图聚合出的已收盘 1m K 线，与 GET /fapi/v1/klines 同一根 K 线比较。

  python reconcile.py --app http://127.0.0.1:8765 --symbol ETHUSDT --bars 10   # 校验运行中的程序（WS+REST 混合数据）
  python reconcile.py --symbol BTCUSDT --bars 5                                # 独立模式：直接用 REST aggTrades 重新聚合
  python reconcile.py --app http://127.0.0.1:8765 --start "2026-10-04 09:00" --bars 30   # 校验某个历史时段（本机时区）

比较项：总成交量 vs volume、主动买量 vs taker buy base asset volume、成交笔数（aggTrade 每条含 l-f+1 笔原始成交，仅独立模式可比）。
"""
from __future__ import annotations

import argparse
import asyncio
import time
from decimal import Decimal

import aiohttp

from fp_binance import BinanceRest, load_specs, make_session, resolve_proxy
from fp_core import Book, build_bars


async def bars_from_app(app: str, symbol: str) -> tuple[list[dict], dict]:
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{app}/api/bars", params={"symbol": symbol, "interval": "1m", "limit": "100000"}) as r:
            d = await r.json()
    return d["bars"], d["spec"]


async def bars_from_rest(rest: BinanceRest, spec, start: int, end: int) -> tuple[list[dict], dict]:
    book = Book(spec)
    raw_trades: dict[int, int] = {}
    rows = await rest.agg_trades(spec.symbol, start_time=start, end_time=min(end, start + 3600_000 - 1))
    while rows:
        for d in rows:
            if d["T"] > end:
                rows = []
                break
            if book.apply_raw(d):
                k = d["T"] // 60000 * 60000
                raw_trades[k] = raw_trades.get(k, 0) + d["l"] - d["f"] + 1
        if not rows or len(rows) < 1000:
            break
        rows = await rest.agg_trades(spec.symbol, from_id=book.last_id + 1)
    bars = build_bars(book, 2, 1)
    for b in bars:
        b["raw_n"] = raw_trades.get(b["t"])
    return bars, spec.meta()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="ETHUSDT")
    ap.add_argument("--bars", type=int, default=10, help="比较最近多少根已收盘 1m K 线")
    ap.add_argument("--app", default=None, help="运行中的足迹图地址；不填则独立用 REST 聚合")
    ap.add_argument("--start", default=None, help="从这个本机时间的 K 线开始比较，如 '2026-10-04 09:00'（默认取最近已收盘的）")
    ap.add_argument("--proxy", default=None)
    a = ap.parse_args()

    session, req_proxy = make_session(resolve_proxy(a.proxy))
    rest = BinanceRest(session, req_proxy)
    try:
        now = int(time.time() * 1000)
        cur = now // 60000 * 60000                     # 当前未收盘 K 线开盘时间
        start, end = cur - a.bars * 60000, cur - 1
        if a.start:
            start = int(time.mktime(time.strptime(a.start, "%Y-%m-%d %H:%M")) * 1000)
            end = min(cur, start + a.bars * 60000) - 1
        if a.app:
            bars, meta = await bars_from_app(a.app, a.symbol)
            source = f"运行中程序 {a.app}"
        else:
            spec = load_specs(await rest.exchange_info(), [a.symbol])[a.symbol]
            bars, meta = await bars_from_rest(rest, spec, start, end)
            source = "REST aggTrades 独立聚合"
        kl = await rest.klines(a.symbol, "1m", start, end, limit=a.bars)
    finally:
        await session.close()

    q = Decimal(1).scaleb(-meta["qtyDec"])
    mine = {b["t"]: b for b in bars}
    print(f"\n{a.symbol} 1m 对账（{source}）  数量单位 {meta['stepSize']}")
    print(f"{'开盘时间(本地)':<20}{'klines volume':>16}{'足迹 总量':>16}{'差':>10}{'klines takerBuy':>18}{'足迹 主动买':>16}{'差':>10}{'笔数(k/足迹)':>18}")
    worst = Decimal(0)
    ok = 0
    for k in kl:
        t = k[0]
        vol, tb, n = Decimal(k[5]), Decimal(k[9]), k[8]
        b = mine.get(t)
        ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(t / 1000))
        if not b:
            print(f"{ts:<20}{vol:>16}{'（无数据）':>16}")
            continue
        fv, fb = b["v"] * q, b["buy"] * q
        dv, db = fv - vol, fb - tb
        rel = max(abs(dv) / vol if vol else 0, abs(db) / tb if tb else 0)
        worst = max(worst, rel)
        ok += 1
        cnt = f"{n}/{b.get('raw_n', '-')}"
        print(f"{ts:<20}{vol:>16}{fv:>16}{dv:>10}{tb:>18}{fb:>16}{db:>10}{cnt:>18}")
    print(f"\n比较 {ok} 根，最大相对误差 {worst * 100:.6f}%")


if __name__ == "__main__":
    asyncio.run(main())
