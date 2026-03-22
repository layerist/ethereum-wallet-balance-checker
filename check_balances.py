#!/usr/bin/env python3
"""
Ethereum Wallet Balance Checker (Ultra Hardened)

Key Improvements:
- HTTP connection pooling (requests.Session)
- Optional batch RPC calls (massive speed boost)
- Advanced retry with jitter
- Rate limiting protection
- Partial result saving on interrupt
- Metrics (success rate, latency)
- CSV + JSON output
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
from typing import Callable, Dict, Iterable, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from web3 import Web3
from web3.exceptions import InvalidAddress

# =========================
# Constants
# =========================
ETH_DECIMALS = 6
RETRY_ATTEMPTS = 5
BASE_DELAY = 0.4
MAX_DELAY = 5
WEI_IN_ETH = Decimal("1000000000000000000")

getcontext().prec = 28

# =========================
# Thread-local session + web3
# =========================
_thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session
    return _thread_local.session


def get_web3(node_url: str) -> Web3:
    if not hasattr(_thread_local, "web3"):
        session = get_session()
        _thread_local.web3 = Web3(
            Web3.HTTPProvider(
                node_url,
                session=session,
                request_kwargs={"timeout": 10},
            )
        )
    return _thread_local.web3


# =========================
# Data model
# =========================
@dataclass(frozen=True)
class BalanceResult:
    address: str
    balance_wei: Optional[int]
    balance_eth: Optional[Decimal]
    error: Optional[str] = None
    latency_ms: Optional[float] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        if self.balance_eth is not None:
            d["balance_eth"] = f"{self.balance_eth:.{ETH_DECIMALS}f}"
        return d


# =========================
# Retry with jitter
# =========================
def with_retries(fn: Callable[[], int]) -> int:
    delay = BASE_DELAY

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == RETRY_ATTEMPTS:
                raise

            sleep_time = delay + random.uniform(0, 0.3)
            logging.debug(f"Retry {attempt} failed: {type(e).__name__}, sleeping {sleep_time:.2f}s")

            time.sleep(sleep_time)
            delay = min(delay * 2, MAX_DELAY)

    raise RuntimeError("Retry failed")


# =========================
# Single fetch
# =========================
def fetch_balance(node_url: str, address: str) -> BalanceResult:
    start = time.perf_counter()

    try:
        web3 = get_web3(node_url)

        wei = with_retries(lambda: web3.eth.get_balance(address))
        eth = Decimal(wei) / WEI_IN_ETH

        latency = (time.perf_counter() - start) * 1000

        return BalanceResult(address, wei, eth, latency_ms=latency)

    except InvalidAddress:
        return BalanceResult(address, None, None, "InvalidAddress")

    except Exception as e:
        return BalanceResult(address, None, None, type(e).__name__)


# =========================
# Batch RPC (optional)
# =========================
def fetch_batch(node_url: str, addresses: List[str]) -> List[BalanceResult]:
    session = get_session()

    payload = []
    for i, addr in enumerate(addresses):
        payload.append({
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getBalance",
            "params": [addr, "latest"]
        })

    try:
        response = session.post(node_url, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()

        results = []
        for item, addr in zip(data, addresses):
            if "result" in item:
                wei = int(item["result"], 16)
                eth = Decimal(wei) / WEI_IN_ETH
                results.append(BalanceResult(addr, wei, eth))
            else:
                results.append(BalanceResult(addr, None, None, "RPCError"))

        return results

    except Exception as e:
        logging.debug("Batch failed, fallback to single mode")
        return [fetch_balance(node_url, a) for a in addresses]


# =========================
# Concurrency
# =========================
def fetch_all(
    node_url: str,
    addresses: List[str],
    workers: int,
    batch_size: int
) -> List[BalanceResult]:

    results: Dict[str, BalanceResult] = {}

    logging.info(f"Workers: {workers} | Batch size: {batch_size}")

    chunks = [
        addresses[i:i + batch_size]
        for i in range(0, len(addresses), batch_size)
    ]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_batch, node_url, chunk): chunk
            for chunk in chunks
        }

        try:
            for future in as_completed(futures):
                chunk = futures[future]
                try:
                    res_list = future.result()
                    for r in res_list:
                        results[r.address] = r
                except Exception as e:
                    for addr in chunk:
                        results[addr] = BalanceResult(addr, None, None, type(e).__name__)

        except KeyboardInterrupt:
            logging.warning("Interrupted! Returning partial results...")
            executor.shutdown(cancel_futures=True)

    return [results[a] for a in addresses]


# =========================
# IO
# =========================
def load_addresses(path: Path) -> List[str]:
    seen = set()
    out = []

    for line in path.read_text().splitlines():
        addr = line.strip()
        if not addr or addr in seen:
            continue
        seen.add(addr)

        if Web3.is_address(addr):
            out.append(Web3.to_checksum_address(addr))

    return out


def save_json(results: List[BalanceResult], path: Path):
    data = {r.address: r.to_dict() for r in results}
    path.write_text(json.dumps(data, indent=2))


def save_csv(results: List[BalanceResult], path: Path):
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["address", "balance_eth", "error", "latency_ms"])
        for r in results:
            writer.writerow([
                r.address,
                r.balance_eth,
                r.error,
                r.latency_ms
            ])


# =========================
# Metrics
# =========================
def print_stats(results: List[BalanceResult], start: float):
    success = sum(1 for r in results if r.error is None)
    total = len(results)

    latencies = [r.latency_ms for r in results if r.latency_ms]
    avg_latency = sum(latencies) / len(latencies) if latencies else 0

    logging.info(f"Success: {success}/{total}")
    logging.info(f"Avg latency: {avg_latency:.2f} ms")
    logging.info(f"Time: {time.time() - start:.2f}s")


# =========================
# CLI
# =========================
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("-i", default="wallets.txt")
    p.add_argument("-o", default="balances.json")
    p.add_argument("--csv", default=None)
    p.add_argument("-n", required=True)
    p.add_argument("--workers", type=int, default=10)
    p.add_argument("--batch", type=int, default=20)
    return p.parse_args()


# =========================
# Main
# =========================
def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO)

    start = time.time()

    try:
        addresses = load_addresses(Path(args.i))

        results = fetch_all(
            args.n,
            addresses,
            args.workers,
            args.batch
        )

        save_json(results, Path(args.o))

        if args.csv:
            save_csv(results, Path(args.csv))

        print_stats(results, start)

    except KeyboardInterrupt:
        logging.warning("Stopped by user")
        sys.exit(130)


if __name__ == "__main__":
    main()
