"""data.binance.vision 每日 aggTrades 历史包：下载、校验、缓存、解析。

路径：https://data.binance.vision/data/futures/um/daily/aggTrades/<SYM>/<SYM>-aggTrades-YYYY-MM-DD.zip
CSV 列：agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker
（近年的包带表头行，早年的没有；解析时跳过首列不是数字的行。）
当天（UTC）的包不存在，前一天的包通常在 UTC 次日早上才生成，不存在时返回 None 由上层退回 REST。
"""
from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import zipfile
from datetime import date
from pathlib import Path

import aiohttp

from fp_core import Book, SymbolSpec

log = logging.getLogger("history")

VISION_BASE = "https://data.binance.vision/data/futures/um/daily/aggTrades"


def zip_name(symbol: str, day: date) -> str:
    return f"{symbol}-aggTrades-{day.isoformat()}.zip"


def iter_zip_rows(path: Path):
    """逐行产出 (a, p, q, T, m) 字符串元组；兼容有无表头。"""
    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            if not name.endswith(".csv"):
                continue
            with z.open(name) as f:
                for row in csv.reader(io.TextIOWrapper(f, "ascii", newline="")):
                    if not row or not row[0].isdigit():
                        continue  # 表头或空行
                    yield row[0], row[1], row[2], row[5], row[6]


def load_zip_book(path: str, spec: SymbolSpec) -> Book:
    """在子进程中运行：把一天的历史包聚合成 Book 并冻结全部槽（便于跨进程传输和省内存）。"""
    b = Book(spec)
    apply, pt, qu = b.apply, spec.price_ticks, spec.qty_units
    last = None
    for a, p, q, T, m in iter_zip_rows(Path(path)):
        ai = int(a)
        if last is not None and ai <= last:
            continue  # 包内重复（理论上不会有）
        apply(ai, int(T), pt(p), qu(q), m == "false" or m == "False")
        last = ai
    b.dirty.clear()
    b.freeze_before(1 << 62)
    return b


class VisionStore:
    """下载并缓存历史包。缓存目录结构与官方路径一致：<cache>/<SYM>/<文件名>.zip"""

    def __init__(self, session: aiohttp.ClientSession, proxy: str | None, cache_dir: Path):
        self.session = session
        self.proxy = proxy
        self.cache_dir = cache_dir

    async def _get(self, url: str) -> bytes | None:
        async with self.session.get(url, proxy=self.proxy,
                                    timeout=aiohttp.ClientTimeout(total=600, sock_connect=15)) as r:
            if r.status in (403, 404):  # 不存在（S3 对不存在的对象可能返回 403 或 404）
                return None
            r.raise_for_status()
            return await r.read()

    async def fetch(self, symbol: str, day: date) -> Path | None:
        """返回本地缓存的 zip 路径；包尚未生成时返回 None。"""
        name = zip_name(symbol, day)
        path = self.cache_dir / symbol / name
        if path.exists() and path.stat().st_size > 0:
            return path
        url = f"{VISION_BASE}/{symbol}/{name}"
        data = await self._get(url)
        if data is None:
            return None
        checksum = await self._get(url + ".CHECKSUM")
        if checksum is not None:
            expect = checksum.decode().split()[0].lower()
            got = hashlib.sha256(data).hexdigest()
            if got != expect:
                raise IOError(f"{name} 校验失败：sha256 {got} != {expect}")
        else:
            log.warning("%s 没有 .CHECKSUM，跳过校验", name)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".part")
        tmp.write_bytes(data)
        os.replace(tmp, path)  # 原子落盘，避免半个文件被当成缓存
        log.info("已下载 %s（%.1f MB）", name, len(data) / 1e6)
        return path
