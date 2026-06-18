#!/usr/bin/env python3
"""
Ethereum Wallet Balance Checker

High-performance hybrid RPC balance checker.

Features:
- Multiple RPC nodes support
- Adaptive node scoring
- Per-node cooldown and circuit breaker
- Retry across different nodes
- Thread-local HTTP sessions
- Writer thread with queue
- Streaming JSONL output
- Fast resume via .done index
- Progress logging
"""

from __future__ import annotations

import argparse
import json
import logging
import queue
import random
import threading
import time
from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import requests
from web3 import Web3


# =========================
# DEFAULT CONFIG
# =========================

WEI = Decimal("1000000000000000000")

DEFAULT_BATCH_SIZE = 50
DEFAULT_WORKERS = 40
DEFAULT_MAX_INFLIGHT = 200
DEFAULT_MAX_RETRIES = 3

DEFAULT_CONNECT_TIMEOUT = 5
DEFAULT_READ_TIMEOUT = 15

BASE_COOLDOWN_SEC = 2.0
RATE_LIMIT_COOLDOWN_MULTIPLIER = 5
MAX_COOLDOWN_SEC = 60

HTTP_POOL_SIZE = 200

PROGRESS_EVERY_SEC = 5
WRITER_FLUSH_EVERY_LINES = 500


# =========================
# THREAD-LOCAL HTTP SESSION
# =========================

_thread_local = threading.local()


def get_session() -> requests.Session:
    """
    requests.Session is not guaranteed to be thread-safe.
    Each worker thread gets its own Session with connection pooling.
    """
    session = getattr(_thread_local, "session", None)

    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=HTTP_POOL_SIZE,
            pool_maxsize=HTTP_POOL_SIZE,
            max_retries=0,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session

    return session


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
    node: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.balance_wei is not None

    def dump(self) -> dict:
        d = asdict(self)

        if self.balance_eth is not None:
            # Читаемый ETH без float, чтобы не ловить ошибки округления.
            d["balance_eth"] = str(
                self.balance_eth.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
            )

        return d


# =========================
# NODE MANAGER
# =========================

@dataclass
class NodeState:
    score: float = 0.0
    cooldown_until: float = 0.0
    fail_streak: int = 0
    success_count: int = 0
    fail_count: int = 0
    rate_limit_count: int = 0
    inflight: int = 0
    ewma_latency_ms: Optional[float] = None


class NodeManager:
    def __init__(self, nodes: List[str]):
        if not nodes:
            raise ValueError("RPC node list is empty")

        self.nodes = nodes
        self.state: Dict[str, NodeState] = {n: NodeState() for n in nodes}
        self.lock = threading.Lock()

    def acquire_node(self, exclude: Optional[Set[str]] = None) -> str:
        """
        Pick best available node.

        Lower score is better.
        Penalizes:
        - cooldown
        - recent failures
        - current inflight
        - slow latency
        """
        exclude = exclude or set()

        while True:
            with self.lock:
                now = time.time()

                candidates = [
                    n for n in self.nodes
                    if n not in exclude and self.state[n].cooldown_until <= now
                ]

                if candidates:
                    node = min(candidates, key=self._effective_score)
                    self.state[node].inflight += 1
                    return node

                # If all non-excluded nodes are cooling down, wait for the soonest one.
                wait_until = min(
                    self.state[n].cooldown_until
                    for n in self.nodes
                    if n not in exclude
                ) if any(n not in exclude for n in self.nodes) else now

            sleep_for = max(0.05, min(1.0, wait_until - time.time()))
            time.sleep(sleep_for)

    def release_node(
        self,
        node: str,
        success: bool,
        latency_ms: Optional[float] = None,
        rate_limited: bool = False,
        server_error: bool = False,
    ) -> None:
        with self.lock:
            st = self.state[node]
            st.inflight = max(0, st.inflight - 1)

            if success:
                st.success_count += 1
                st.fail_streak = 0
                st.score = max(0.0, st.score - 1.0)

                if latency_ms is not None:
                    if st.ewma_latency_ms is None:
                        st.ewma_latency_ms = latency_ms
                    else:
                        st.ewma_latency_ms = st.ewma_latency_ms * 0.8 + latency_ms * 0.2

                return

            st.fail_count += 1
            st.fail_streak += 1

            if rate_limited:
                st.rate_limit_count += 1
                st.score += 8.0
                cooldown = BASE_COOLDOWN_SEC * RATE_LIMIT_COOLDOWN_MULTIPLIER * st.fail_streak
            elif server_error:
                st.score += 4.0
                cooldown = BASE_COOLDOWN_SEC * min(st.fail_streak, 5)
            else:
                st.score += 3.0
                cooldown = BASE_COOLDOWN_SEC * min(st.fail_streak, 5)

            cooldown = min(MAX_COOLDOWN_SEC, cooldown)
            st.cooldown_until = time.time() + cooldown + random.random()

    def _effective_score(self, node: str) -> float:
        st = self.state[node]

        latency_penalty = 0.0
        if st.ewma_latency_ms is not None:
            latency_penalty = st.ewma_latency_ms / 1000.0

        return (
            st.score
            + st.inflight * 2.0
            + st.fail_streak * 5.0
            + latency_penalty
        )

    def snapshot(self) -> dict:
        with self.lock:
            return {
                self._mask_node(n): {
                    "score": round(st.score, 2),
                    "fail_streak": st.fail_streak,
                    "success": st.success_count,
                    "fail": st.fail_count,
                    "429": st.rate_limit_count,
                    "inflight": st.inflight,
                    "latency_ms": round(st.ewma_latency_ms, 1) if st.ewma_latency_ms else None,
                    "cooldown_left": max(0, round(st.cooldown_until - time.time(), 1)),
                }
                for n, st in self.state.items()
            }

    @staticmethod
    def _mask_node(node: str) -> str:
        # Не светим API keys в логах.
        if "://" not in node:
            return node[:20] + "..."

        scheme, rest = node.split("://", 1)

        if "@" in rest:
            auth, host = rest.rsplit("@", 1)
            return f"{scheme}://***@{host}"

        if "/" in rest:
            host, path = rest.split("/", 1)
            return f"{scheme}://{host}/..."

        return f"{scheme}://{rest}"


# =========================
# WRITER THREAD
# =========================

class Writer:
    def __init__(self, out_path: Path, done_path: Path):
        self.out_path = out_path
        self.done_path = done_path

        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self.done_path.parent.mkdir(parents=True, exist_ok=True)

        self.q: queue.Queue[Optional[List[BalanceResult]]] = queue.Queue(maxsize=10_000)
        self.thread = threading.Thread(target=self._run, daemon=True)

        self.written_lines = 0
        self.lock = threading.Lock()

        self.thread.start()

    def push(self, results: List[BalanceResult]) -> None:
        self.q.put(results)

    def stop(self) -> None:
        self.q.put(None)
        self.thread.join()

    def _run(self) -> None:
        buffered = 0

        with self.out_path.open("a", encoding="utf-8") as out_f, \
             self.done_path.open("a", encoding="utf-8") as done_f:

            while True:
                batch = self.q.get()

                if batch is None:
                    break

                for r in batch:
                    out_f.write(json.dumps(r.dump(), ensure_ascii=False) + "\n")

                    # В done-индекс пишем только успешные адреса.
                    # Ошибочные адреса будут повторно обработаны при следующем запуске.
                    if r.ok:
                        done_f.write(r.address + "\n")

                    buffered += 1

                with self.lock:
                    self.written_lines += len(batch)

                if buffered >= WRITER_FLUSH_EVERY_LINES:
                    out_f.flush()
                    done_f.flush()
                    buffered = 0

            out_f.flush()
            done_f.flush()


# =========================
# STATS
# =========================

class Stats:
    def __init__(self, total: int):
        self.total = total
        self.ok = 0
        self.errors = 0
        self.lock = threading.Lock()
        self.started_at = time.perf_counter()

    def add(self, results: List[BalanceResult]) -> None:
        with self.lock:
            for r in results:
                if r.ok:
                    self.ok += 1
                else:
                    self.errors += 1

    def snapshot(self) -> Tuple[int, int, int, float]:
        with self.lock:
            done = self.ok + self.errors
            elapsed = max(0.001, time.perf_counter() - self.started_at)
            speed = done / elapsed
            return done, self.ok, self.errors, speed


# =========================
# CORE RPC
# =========================

class BatchTransportError(Exception):
    pass


def fetch_batch(
    node_mgr: NodeManager,
    addresses: List[str],
    max_retries: int,
    timeout: Tuple[int, int],
) -> List[BalanceResult]:

    last_error: Optional[str] = None
    tried_nodes: Set[str] = set()

    for attempt in range(1, max_retries + 1):
        # Сначала пробуем разные RPC. Если все уже пробовали — разрешаем повтор.
        if len(tried_nodes) >= len(node_mgr.nodes):
            tried_nodes.clear()

        node = node_mgr.acquire_node(exclude=tried_nodes)
        tried_nodes.add(node)

        start = time.perf_counter()

        try:
            results = call_rpc_batch(node, addresses, timeout)
            latency_ms = (time.perf_counter() - start) * 1000

            node_mgr.release_node(node, success=True, latency_ms=latency_ms)

            for r in results:
                r.node = NodeManager._mask_node(node)
                r.latency_ms = latency_ms

            return results

        except requests.HTTPError as e:
            latency_ms = (time.perf_counter() - start) * 1000
            status_code = e.response.status_code if e.response is not None else None

            is_rate_limit = status_code == 429
            is_server_error = status_code in {500, 502, 503, 504}

            node_mgr.release_node(
                node,
                success=False,
                latency_ms=latency_ms,
                rate_limited=is_rate_limit,
                server_error=is_server_error,
            )

            last_error = f"HTTP_{status_code}"

        except (requests.Timeout, requests.ConnectionError) as e:
            latency_ms = (time.perf_counter() - start) * 1000

            node_mgr.release_node(
                node,
                success=False,
                latency_ms=latency_ms,
                server_error=True,
            )

            last_error = type(e).__name__

        except Exception as e:
            latency_ms = (time.perf_counter() - start) * 1000

            node_mgr.release_node(
                node,
                success=False,
                latency_ms=latency_ms,
                server_error=False,
            )

            last_error = str(e)[:200]

        # Небольшой backoff между ретраями.
        time.sleep(min(2.0, 0.2 * attempt + random.random() * 0.3))

    return [
        BalanceResult(
            address=a,
            balance_wei=None,
            balance_eth=None,
            error=f"FAILED: {last_error}",
            latency_ms=0.0,
            node=None,
        )
        for a in addresses
    ]


def call_rpc_batch(
    node: str,
    addresses: List[str],
    timeout: Tuple[int, int],
) -> List[BalanceResult]:
    payload = [
        {
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getBalance",
            "params": [addr, "latest"],
        }
        for i, addr in enumerate(addresses)
    ]

    session = get_session()
    response = session.post(node, json=payload, timeout=timeout)

    if response.status_code == 429:
        raise requests.HTTPError("HTTP 429", response=response)

    if response.status_code in {500, 502, 503, 504}:
        raise requests.HTTPError(f"HTTP {response.status_code}", response=response)

    response.raise_for_status()

    try:
        data = response.json()
    except Exception as e:
        raise BatchTransportError(f"BAD_JSON: {e}") from e

    if isinstance(data, dict):
        # Некоторые RPC могут вернуть одиночную ошибку на batch request.
        if "error" in data:
            msg = data["error"].get("message", "RPC_ERROR")
            raise BatchTransportError(f"RPC_BATCH_ERROR: {msg}")

        raise BatchTransportError("UNEXPECTED_JSON_OBJECT")

    if not isinstance(data, list):
        raise BatchTransportError("UNEXPECTED_JSON_TYPE")

    mapping = {}
    for item in data:
        if isinstance(item, dict) and "id" in item:
            mapping[item["id"]] = item

    results: List[BalanceResult] = []

    for i, addr in enumerate(addresses):
        item = mapping.get(i)

        if not item:
            results.append(
                BalanceResult(
                    address=addr,
                    balance_wei=None,
                    balance_eth=None,
                    error="MISSING_RPC_RESPONSE",
                    latency_ms=0.0,
                )
            )
            continue

        if "error" in item:
            err = item.get("error") or {}
            msg = err.get("message") if isinstance(err, dict) else str(err)

            results.append(
                BalanceResult(
                    address=addr,
                    balance_wei=None,
                    balance_eth=None,
                    error=f"RPC_ERROR: {msg}",
                    latency_ms=0.0,
                )
            )
            continue

        raw_balance = item.get("result")

        if not isinstance(raw_balance, str):
            results.append(
                BalanceResult(
                    address=addr,
                    balance_wei=None,
                    balance_eth=None,
                    error="BAD_RPC_RESULT",
                    latency_ms=0.0,
                )
            )
            continue

        try:
            wei = int(raw_balance, 16)
        except ValueError:
            results.append(
                BalanceResult(
                    address=addr,
                    balance_wei=None,
                    balance_eth=None,
                    error=f"BAD_HEX_BALANCE: {raw_balance}",
                    latency_ms=0.0,
                )
            )
            continue

        eth = Decimal(wei) / WEI

        results.append(
            BalanceResult(
                address=addr,
                balance_wei=wei,
                balance_eth=eth,
                error=None,
                latency_ms=0.0,
            )
        )

    return results


# =========================
# FILE HELPERS
# =========================

def load_addresses(path: Path) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()

            if not raw:
                continue

            # Позволяет использовать файлы вида:
            # address
            # address,comment
            # address;comment
            candidate = raw.split(",")[0].split(";")[0].strip()

            if not Web3.is_address(candidate):
                continue

            checksum = Web3.to_checksum_address(candidate)

            if checksum in seen:
                continue

            seen.add(checksum)
            out.append(checksum)

    return out


def load_done(out_path: Path, done_path: Path) -> Set[str]:
    """
    Fast resume priority:
    1. .done index
    2. fallback scan of JSONL output

    Only successful records are considered done.
    """
    done: Set[str] = set()

    if done_path.exists():
        with done_path.open("r", encoding="utf-8") as f:
            for line in f:
                addr = line.strip()
                if Web3.is_address(addr):
                    done.add(Web3.to_checksum_address(addr))

        return done

    if not out_path.exists():
        return done

    with out_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                addr = obj.get("address")
                err = obj.get("error")

                if addr and err is None and Web3.is_address(addr):
                    done.add(Web3.to_checksum_address(addr))
            except Exception:
                continue

    return done


def parse_nodes(raw: str) -> List[str]:
    nodes = []

    for item in raw.split(","):
        node = item.strip()
        if not node:
            continue
        nodes.append(node)

    # Дедупликация с сохранением порядка.
    return list(dict.fromkeys(nodes))


def chunks_of(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


# =========================
# MAIN ENGINE
# =========================

def run(
    nodes: List[str],
    addresses: List[str],
    workers: int,
    batch_size: int,
    max_inflight: int,
    max_retries: int,
    out_path: Path,
    done_path: Path,
    timeout: Tuple[int, int],
) -> None:
    node_mgr = NodeManager(nodes)

    done = load_done(out_path, done_path)
    addresses = [a for a in addresses if a not in done]

    total = len(addresses)
    stats = Stats(total)
    writer = Writer(out_path, done_path)

    logging.info("RPC nodes: %s", len(nodes))
    logging.info("Already done: %s", len(done))
    logging.info("Remaining wallets: %s", total)
    logging.info("Workers: %s | batch: %s | max_inflight: %s", workers, batch_size, max_inflight)

    if total == 0:
        writer.stop()
        logging.info("Nothing to do")
        return

    chunks = list(chunks_of(addresses, batch_size))
    futures = set()

    last_progress_log = time.perf_counter()

    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            i = 0

            while i < len(chunks) or futures:
                while i < len(chunks) and len(futures) < max_inflight:
                    futures.add(
                        ex.submit(
                            fetch_batch,
                            node_mgr,
                            chunks[i],
                            max_retries,
                            timeout,
                        )
                    )
                    i += 1

                done_futures, futures = wait(futures, return_when=FIRST_COMPLETED)

                for f in done_futures:
                    try:
                        results = f.result()
                    except Exception as e:
                        logging.exception("Unexpected worker error: %s", e)
                        continue

                    stats.add(results)
                    writer.push(results)

                now = time.perf_counter()
                if now - last_progress_log >= PROGRESS_EVERY_SEC:
                    done_count, ok_count, err_count, speed = stats.snapshot()
                    remaining = max(0, total - done_count)

                    logging.info(
                        "Progress: %s/%s | ok=%s | err=%s | remaining=%s | %.1f wallets/sec",
                        done_count,
                        total,
                        ok_count,
                        err_count,
                        remaining,
                        speed,
                    )

                    last_progress_log = now

    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Already written results are saved.")

    finally:
        writer.stop()

    done_count, ok_count, err_count, speed = stats.snapshot()

    logging.info(
        "Completed: %s/%s | ok=%s | err=%s | avg_speed=%.1f wallets/sec",
        done_count,
        total,
        ok_count,
        err_count,
        speed,
    )

    logging.info("Node stats: %s", json.dumps(node_mgr.snapshot(), ensure_ascii=False))


# =========================
# CLI
# =========================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ethereum wallet balance checker with multi-RPC batching and resume"
    )

    parser.add_argument("-i", "--input", default="wallets.txt", help="Input wallets file")
    parser.add_argument("-o", "--output", default="balances.jsonl", help="Output JSONL file")
    parser.add_argument("-n", "--nodes", required=True, help="RPC URLs comma separated")

    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-inflight", type=int, default=DEFAULT_MAX_INFLIGHT)
    parser.add_argument("--retries", type=int, default=DEFAULT_MAX_RETRIES)

    parser.add_argument("--connect-timeout", type=int, default=DEFAULT_CONNECT_TIMEOUT)
    parser.add_argument("--read-timeout", type=int, default=DEFAULT_READ_TIMEOUT)

    parser.add_argument(
        "--done-index",
        default=None,
        help="Optional done index path. Default: output + '.done'",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    nodes = parse_nodes(args.nodes)

    if not nodes:
        raise SystemExit("No RPC nodes provided")

    input_path = Path(args.input)
    output_path = Path(args.output)

    done_path = Path(args.done_index) if args.done_index else Path(str(output_path) + ".done")

    addresses = load_addresses(input_path)

    if not addresses:
        raise SystemExit("No valid Ethereum addresses found")

    run(
        nodes=nodes,
        addresses=addresses,
        workers=args.workers,
        batch_size=args.batch,
        max_inflight=args.max_inflight,
        max_retries=args.retries,
        out_path=output_path,
        done_path=done_path,
        timeout=(args.connect_timeout, args.read_timeout),
    )


if __name__ == "__main__":
    main()
