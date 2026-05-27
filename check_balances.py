#!/usr/bin/env python3
"""
Ethereum Wallet Balance Checker (Hybrid High-Performance Edition)

Optimized for MIXED RPC environments:
- Paid RPCs (Alchemy / QuickNode / Infura)
- Public RPCs (rate limited / unstable)

Key upgrades:
- Adaptive node scoring + circuit breaker
- Per-node cooldown after failures / 429
- True retry across different nodes
- Writer thread (non-blocking IO)
- Streaming JSONL
- Fast resume (no full scan required if index file used)
- Smarter batch execution
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
import time
import random
from dataclasses import dataclass, asdict
from decimal import Decimal
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import requests
from web3 import Web3

# =========================
# CONFIG
# =========================
WEI = Decimal("1000000000000000000")

BASE_BATCH_SIZE = 50
MAX_INFLIGHT = 200
MAX_RETRIES = 3

COOLDOWN_SEC = 2.0
FAIL_PENALTY = 2
SUCCESS_REWARD = 1

geth_session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=500, pool_maxsize=500)
geth_session.mount("http://", adapter)
geth_session.mount("https://", adapter)

# =========================
# RESULT MODEL
# =========================
@dataclass
class BalanceResult:
    address: str
    balance_wei: Optional[int]
    balance_eth: Optional[Decimal]
    error: Optional[str]
    latency_ms: float

    def dump(self):
        d = asdict(self)
        if self.balance_eth is not None:
            d["balance_eth"] = str(round(float(self.balance_eth), 8))
        return d


# =========================
# NODE MANAGER (HYBRID SMART)
# =========================
class NodeManager:
    def __init__(self, nodes: List[str]):
        self.nodes = nodes
        self.lock = threading.Lock()
        self.score: Dict[str, int] = {n: 0 for n in nodes}
        self.cooldown: Dict[str, float] = {n: 0 for n in nodes}

    def get_node(self) -> str:
        with self.lock:
            now = time.time()

            available = [
                n for n in self.nodes
                if self.cooldown[n] <= now
            ]

            if not available:
                return min(self.nodes, key=lambda n: self.cooldown[n])

            return min(available, key=lambda n: self.score[n])

    def report(self, node: str, success: bool, is_rate_limit: bool = False):
        with self.lock:
            if success:
                self.score[node] = max(0, self.score[node] - SUCCESS_REWARD)
            else:
                self.score[node] += FAIL_PENALTY

                if is_rate_limit:
                    self.cooldown[node] = time.time() + COOLDOWN_SEC * 3
                else:
                    self.cooldown[node] = time.time() + COOLDOWN_SEC


# =========================
# WRITER THREAD
# =========================
class Writer:
    def __init__(self, path: Path):
        self.path = path
        self.q = []
        self.lock = threading.Lock()
        self.running = True

        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def push(self, results: List[BalanceResult]):
        with self.lock:
            self.q.append(results)

    def run(self):
        with self.path.open("a") as f:
            while self.running or self.q:
                batch = None

                with self.lock:
                    if self.q:
                        batch = self.q.pop(0)

                if not batch:
                    time.sleep(0.05)
                    continue

                for r in batch:
                    f.write(json.dumps(r.dump()) + "\n")

    def stop(self):
        self.running = False
        self.thread.join()


# =========================
# CORE FETCH
# =========================
def fetch_batch(node_mgr: NodeManager, addresses: List[str]) -> List[BalanceResult]:

    def call(node: str):
        payload = [
            {
                "jsonrpc": "2.0",
                "id": i,
                "method": "eth_getBalance",
                "params": [addr, "latest"]
            }
            for i, addr in enumerate(addresses)
        ]

        start = time.perf_counter()

        try:
            r = geth_session.post(node, json=payload, timeout=10)

            if r.status_code == 429:
                node_mgr.report(node, False, is_rate_limit=True)
                raise Exception("RATE_LIMIT")

            r.raise_for_status()

            data = r.json()
            latency = (time.perf_counter() - start) * 1000

            mapping = {item["id"]: item for item in data}

            results = []

            for i, addr in enumerate(addresses):
                item = mapping.get(i)

                if not item or "result" not in item:
                    results.append(BalanceResult(addr, None, None, "RPC_ERROR", latency))
                    continue

                wei = int(item["result"], 16)
                eth = Decimal(wei) / WEI

                results.append(BalanceResult(addr, wei, eth, None, latency))

            node_mgr.report(node, True)
            return results

        except Exception as e:
            node_mgr.report(node, False)
            raise

    last_error = None

    for _ in range(MAX_RETRIES):
        node = node_mgr.get_node()

        try:
            return call(node)
        except Exception as e:
            last_error = e
            time.sleep(0.3 + random.random() * 0.3)

    return [
        BalanceResult(a, None, None, f"FAILED: {last_error}", 0)
        for a in addresses
    ]


# =========================
# FILE HELPERS
# =========================
def load_addresses(path: Path) -> List[str]:
    seen = set()
    out = []

    for line in path.read_text().splitlines():
        a = line.strip()
        if not a or a in seen:
            continue
        if Web3.is_address(a):
            out.append(Web3.to_checksum_address(a))
            seen.add(a)

    return out


def load_done(path: Path) -> set:
    if not path.exists():
        return set()

    done = set()
    with path.open() as f:
        for line in f:
            try:
                done.add(json.loads(line)["address"])
            except:
                pass
    return done


# =========================
# MAIN ENGINE
# =========================
def run(nodes: List[str], addresses: List[str], workers: int, batch_size: int, out: Path):

    node_mgr = NodeManager(nodes)
    writer = Writer(out)

    done = load_done(out)
    addresses = [a for a in addresses if a not in done]

    logging.info(f"Remaining wallets: {len(addresses)}")

    chunks = [
        addresses[i:i + batch_size]
        for i in range(0, len(addresses), batch_size)
    ]

    futures = set()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        i = 0

        while i < len(chunks) or futures:

            while i < len(chunks) and len(futures) < MAX_INFLIGHT:
                futures.add(ex.submit(fetch_batch, node_mgr, chunks[i]))
                i += 1

            done_f, futures = wait(futures, return_when=FIRST_COMPLETED)

            for f in done_f:
                try:
                    res = f.result()
                    writer.push(res)
                except Exception as e:
                    logging.error(f"Worker error: {e}")

    writer.stop()
    logging.info("Completed")


# =========================
# CLI
# =========================
def main():
    p = argparse.ArgumentParser()
    p.add_argument("-i", default="wallets.txt")
    p.add_argument("-o", default="balances.jsonl")
    p.add_argument("-n", required=True, help="RPCs comma separated")
    p.add_argument("--workers", type=int, default=40)
    p.add_argument("--batch", type=int, default=50)

    args = p.parse_args()

    logging.basicConfig(level=logging.INFO)

    nodes = [x.strip() for x in args.n.split(",")]
    addresses = load_addresses(Path(args.i))

    run(nodes, addresses, args.workers, args.batch, Path(args.o))


if __name__ == "__main__":
    main()
