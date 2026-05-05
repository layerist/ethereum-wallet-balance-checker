#!/usr/bin/env python3
"""
Ethereum Wallet Balance Checker (Ultra Hardened v3)

Major upgrades over v2:
- True multi-node failover (retry across nodes)
- Streaming JSONL output (no RAM explosion)
- Resume support (skip already processed)
- Bounded task queue (no node overload)
- Node health scoring (bad nodes penalized)
- Safe unordered batch handling
- Much faster checkpointing
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import threading
import random
import csv

from dataclasses import dataclass, asdict
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Callable, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from collections import defaultdict, deque

import requests
from web3 import Web3
from web3.exceptions import InvalidAddress

# =========================
# CONFIG
# =========================
ETH_DECIMALS = 6
RETRY_ATTEMPTS = 3
BASE_DELAY = 0.2
MAX_DELAY = 3

WEI_IN_ETH = Decimal("1000000000000000000")

MAX_INFLIGHT = 200  # prevents overload
RATE_WINDOW = 50

getcontext().prec = 28

# =========================
# GLOBAL SESSION
# =========================
_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=300,
    pool_maxsize=300,
    max_retries=0,
)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)

# =========================
# THREAD-LOCAL WEB3
# =========================
_thread_local = threading.local()


def get_web3(node_url: str) -> Web3:
    if not hasattr(_thread_local, "cache"):
        _thread_local.cache = {}

    if node_url not in _thread_local.cache:
        _thread_local.cache[node_url] = Web3(
            Web3.HTTPProvider(
                node_url,
                session=_session,
                request_kwargs={"timeout": 10},
            )
        )

    return _thread_local.cache[node_url]


# =========================
# RATE LIMITER
# =========================
class AdaptiveRateLimiter:
    def __init__(self):
        self.lock = threading.Lock()
        self.errors = deque(maxlen=RATE_WINDOW)
        self.sleep_time = 0

    def record(self, success: bool):
        with self.lock:
            self.errors.append(0 if success else 1)
            err_rate = sum(self.errors) / len(self.errors)

            if err_rate > 0.3:
                self.sleep_time = min(self.sleep_time + 0.05, 1.5)
            else:
                self.sleep_time = max(self.sleep_time - 0.02, 0)

    def wait(self):
        if self.sleep_time > 0:
            time.sleep(self.sleep_time)


rate_limiter = AdaptiveRateLimiter()

# =========================
# NODE POOL (SMART)
# =========================
class NodePool:
    def __init__(self, nodes: List[str]):
        self.nodes = nodes
        self.lock = threading.Lock()
        self.health = {n: 0 for n in nodes}

    def get(self) -> str:
        with self.lock:
            # pick least "bad" node
            return min(self.nodes, key=lambda n: self.health[n])

    def report(self, node: str, success: bool):
        with self.lock:
            if success:
                self.health[node] = max(self.health[node] - 1, 0)
            else:
                self.health[node] += 1


# =========================
# DATA MODEL
# =========================
@dataclass
class BalanceResult:
    address: str
    balance_wei: Optional[int]
    balance_eth: Optional[Decimal]
    error: Optional[str] = None
    latency_ms: Optional[float] = None

    def to_dict(self):
        d = asdict(self)
        if self.balance_eth is not None:
            d["balance_eth"] = f"{self.balance_eth:.{ETH_DECIMALS}f}"
        return d


# =========================
# RETRY WITH FAILOVER
# =========================
def retry_with_failover(
    node_pool: NodePool,
    fn: Callable[[str], int],
):
    delay = BASE_DELAY

    for attempt in range(RETRY_ATTEMPTS):
        node = node_pool.get()

        try:
            result = fn(node)
            node_pool.report(node, True)
            return result

        except Exception:
            node_pool.report(node, False)

            if attempt == RETRY_ATTEMPTS - 1:
                raise

            time.sleep(delay + random.uniform(0, 0.2))
            delay = min(delay * 2, MAX_DELAY)


# =========================
# BATCH FETCH
# =========================
def fetch_batch(node_pool: NodePool, addresses: List[str]) -> List[BalanceResult]:
    rate_limiter.wait()

    def call(node_url: str):
        payload = []
        for i, addr in enumerate(addresses):
            payload.append({
                "jsonrpc": "2.0",
                "id": i,
                "method": "eth_getBalance",
                "params": [addr, "latest"]
            })

        start = time.perf_counter()

        r = _session.post(node_url, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()

        latency = (time.perf_counter() - start) * 1000

        # SAFE unordered handling
        result_map = {}
        for item in data:
            result_map[item["id"]] = item

        results = []

        for i, addr in enumerate(addresses):
            item = result_map.get(i)

            if not item or "result" not in item:
                results.append(BalanceResult(addr, None, None, "RPCError"))
                continue

            wei = int(item["result"], 16)
            eth = Decimal(wei) / WEI_IN_ETH

            results.append(BalanceResult(addr, wei, eth, latency_ms=latency))

        return results

    try:
        res = retry_with_failover(node_pool, call)
        rate_limiter.record(True)
        return res

    except Exception:
        rate_limiter.record(False)
        return [
            BalanceResult(a, None, None, "Failed")
            for a in addresses
        ]


# =========================
# IO (STREAMING JSONL)
# =========================
def load_existing(path: Path) -> set:
    if not path.exists():
        return set()

    processed = set()
    with path.open() as f:
        for line in f:
            try:
                obj = json.loads(line)
                processed.add(obj["address"])
            except:
                continue

    return processed


def append_jsonl(path: Path, results: List[BalanceResult]):
    with path.open("a") as f:
        for r in results:
            f.write(json.dumps(r.to_dict()) + "\n")


# =========================
# MAIN PROCESSOR
# =========================
def fetch_all(
    nodes: List[str],
    addresses: List[str],
    workers: int,
    batch_size: int,
    output_path: Path,
):
    node_pool = NodePool(nodes)

    done = load_existing(output_path)
    addresses = [a for a in addresses if a not in done]

    logging.info(f"Remaining: {len(addresses)}")

    chunks = [
        addresses[i:i + batch_size]
        for i in range(0, len(addresses), batch_size)
    ]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = set()

        i = 0

        while i < len(chunks) or futures:
            while i < len(chunks) and len(futures) < MAX_INFLIGHT:
                futures.add(executor.submit(fetch_batch, node_pool, chunks[i]))
                i += 1

            done_futures, futures = wait(futures, return_when=FIRST_COMPLETED)

            for f in done_futures:
                try:
                    res = f.result()
                    append_jsonl(output_path, res)
                except Exception as e:
                    logging.error(f"Worker crash: {e}")

    logging.info("Completed")


# =========================
# LOAD ADDRESSES
# =========================
def load_addresses(path: Path) -> List[str]:
    seen = set()
    out = []

    for line in path.read_text().splitlines():
        addr = line.strip()
        if not addr or addr in seen:
            continue

        if Web3.is_address(addr):
            out.append(Web3.to_checksum_address(addr))
            seen.add(addr)

    return out


# =========================
# CSV EXPORT
# =========================
def jsonl_to_csv(jsonl_path: Path, csv_path: Path):
    with jsonl_path.open() as f, csv_path.open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["address", "balance_eth", "error", "latency_ms"])

        for line in f:
            obj = json.loads(line)
            writer.writerow([
                obj["address"],
                obj.get("balance_eth"),
                obj.get("error"),
                obj.get("latency_ms"),
            ])


# =========================
# CLI
# =========================
def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("-i", default="wallets.txt")
    p.add_argument("-o", default="balances.jsonl")
    p.add_argument("--csv", default=None)

    p.add_argument("-n", required=True,
                   help="Comma-separated RPC endpoints")

    p.add_argument("--workers", type=int, default=30)
    p.add_argument("--batch", type=int, default=50)

    return p.parse_args()


# =========================
# MAIN
# =========================
def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO)

    start = time.time()

    try:
        nodes = [x.strip() for x in args.n.split(",")]
        addresses = load_addresses(Path(args.i))

        fetch_all(
            nodes,
            addresses,
            args.workers,
            args.batch,
            Path(args.o),
        )

        if args.csv:
            jsonl_to_csv(Path(args.o), Path(args.csv))

        logging.info(f"Done in {time.time() - start:.2f}s")

    except KeyboardInterrupt:
        logging.warning("Stopped by user")
        sys.exit(130)


if __name__ == "__main__":
    main()
