#!/usr/bin/env python3
"""
Ethereum Wallet Balance Checker v2

Reliable high-throughput Ethereum balance checker using JSON-RPC batch requests.

Highlights:
- Multiple RPC endpoints with adaptive scoring and circuit breaking
- Per-node concurrency limits and latency EWMA
- Retry on a different RPC endpoint
- Partial batch recovery for missing / failed RPC items
- Thread-local HTTP sessions with connection pooling
- Bounded producer/worker/writer pipeline
- Streaming JSONL output and durable resume index
- Graceful Ctrl+C shutdown without losing completed results
- Input validation, deduplication, optional zero-balance filtering
- Periodic progress and per-node diagnostics

Python: 3.10+
Dependencies: requests, web3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import queue
import random
import signal
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from web3 import Web3

WEI = Decimal("1000000000000000000")
ETH_QUANT = Decimal("0.000000000000000001")

DEFAULT_BATCH_SIZE = 50
DEFAULT_WORKERS = 40
DEFAULT_MAX_INFLIGHT = 80
DEFAULT_MAX_RETRIES = 4
DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 15.0
DEFAULT_NODE_CONCURRENCY = 8

BASE_COOLDOWN_SEC = 1.5
MAX_COOLDOWN_SEC = 90.0
HTTP_POOL_SIZE = 64
PROGRESS_EVERY_SEC = 5.0
WRITER_FLUSH_EVERY_LINES = 250
WRITER_FLUSH_EVERY_SEC = 2.0

_thread_local = threading.local()


def monotonic() -> float:
    return time.monotonic()


def get_session(user_agent: str) -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=HTTP_POOL_SIZE,
            pool_maxsize=HTTP_POOL_SIZE,
            max_retries=0,
            pool_block=True,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": user_agent,
            }
        )
        _thread_local.session = session
    return session


@dataclass(slots=True)
class BalanceResult:
    address: str
    balance_wei: Optional[int]
    balance_eth: Optional[Decimal]
    error: Optional[str]
    latency_ms: float
    node: Optional[str] = None
    attempts: int = 1

    @property
    def ok(self) -> bool:
        return self.error is None and self.balance_wei is not None

    def dump(self) -> dict:
        data = asdict(self)
        if self.balance_eth is not None:
            data["balance_eth"] = str(
                self.balance_eth.quantize(ETH_QUANT, rounding=ROUND_DOWN)
            )
        return data


@dataclass(slots=True)
class NodeState:
    score: float = 0.0
    cooldown_until: float = 0.0
    fail_streak: int = 0
    success_count: int = 0
    fail_count: int = 0
    rate_limit_count: int = 0
    inflight: int = 0
    ewma_latency_ms: Optional[float] = None


class NodeUnavailable(RuntimeError):
    pass


class BatchTransportError(RuntimeError):
    pass


class NodeManager:
    def __init__(self, nodes: Sequence[str], per_node_limit: int):
        if not nodes:
            raise ValueError("RPC node list is empty")
        if per_node_limit < 1:
            raise ValueError("per_node_limit must be >= 1")

        self.nodes = list(nodes)
        self.per_node_limit = per_node_limit
        self.state: Dict[str, NodeState] = {node: NodeState() for node in self.nodes}
        self._cv = threading.Condition()

    def acquire_node(
        self,
        exclude: Optional[Set[str]],
        stop_event: threading.Event,
    ) -> str:
        excluded = exclude or set()

        with self._cv:
            while not stop_event.is_set():
                now = monotonic()
                eligible = [
                    node
                    for node in self.nodes
                    if node not in excluded
                    and self.state[node].cooldown_until <= now
                    and self.state[node].inflight < self.per_node_limit
                ]

                if eligible:
                    node = min(eligible, key=self._effective_score)
                    self.state[node].inflight += 1
                    return node

                available_pool = [node for node in self.nodes if node not in excluded]
                if not available_pool:
                    raise NodeUnavailable("all nodes excluded")

                wake_in = 0.25
                cooldowns = [
                    self.state[node].cooldown_until - now
                    for node in available_pool
                    if self.state[node].cooldown_until > now
                ]
                if cooldowns:
                    wake_in = max(0.05, min(0.5, min(cooldowns)))
                self._cv.wait(timeout=wake_in)

        raise NodeUnavailable("shutdown requested")

    def release_node(
        self,
        node: str,
        *,
        success: bool,
        latency_ms: Optional[float] = None,
        rate_limited: bool = False,
        server_error: bool = False,
    ) -> None:
        with self._cv:
            state = self.state[node]
            state.inflight = max(0, state.inflight - 1)

            if latency_ms is not None:
                if state.ewma_latency_ms is None:
                    state.ewma_latency_ms = latency_ms
                else:
                    state.ewma_latency_ms = state.ewma_latency_ms * 0.80 + latency_ms * 0.20

            if success:
                state.success_count += 1
                state.fail_streak = 0
                state.score = max(0.0, state.score * 0.92 - 0.25)
            else:
                state.fail_count += 1
                state.fail_streak += 1

                if rate_limited:
                    state.rate_limit_count += 1
                    state.score += 10.0
                    cooldown = BASE_COOLDOWN_SEC * (3.0 + state.fail_streak * 2.0)
                elif server_error:
                    state.score += 5.0
                    cooldown = BASE_COOLDOWN_SEC * (2 ** min(state.fail_streak - 1, 5))
                else:
                    state.score += 3.0
                    cooldown = BASE_COOLDOWN_SEC * min(state.fail_streak, 5)

                state.cooldown_until = monotonic() + min(MAX_COOLDOWN_SEC, cooldown) + random.random()

            self._cv.notify_all()

    def wake_all(self) -> None:
        with self._cv:
            self._cv.notify_all()

    def _effective_score(self, node: str) -> float:
        state = self.state[node]
        latency_penalty = (state.ewma_latency_ms or 0.0) / 750.0
        utilization_penalty = (state.inflight / self.per_node_limit) * 4.0
        return state.score + state.fail_streak * 4.0 + latency_penalty + utilization_penalty

    def snapshot(self) -> dict:
        with self._cv:
            now = monotonic()
            return {
                mask_url(node): {
                    "score": round(state.score, 2),
                    "fail_streak": state.fail_streak,
                    "success": state.success_count,
                    "fail": state.fail_count,
                    "429": state.rate_limit_count,
                    "inflight": state.inflight,
                    "latency_ms": (
                        round(state.ewma_latency_ms, 1)
                        if state.ewma_latency_ms is not None
                        else None
                    ),
                    "cooldown_left": max(0.0, round(state.cooldown_until - now, 1)),
                }
                for node, state in self.state.items()
            }


def mask_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        if not parts.scheme or not parts.netloc:
            return url[:24] + ("..." if len(url) > 24 else "")

        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        netloc = f"***@{host}" if parts.username or parts.password else host
        path = "/..." if parts.path not in {"", "/"} else parts.path
        return urlunsplit((parts.scheme, netloc, path, "", ""))
    except Exception:
        digest = hashlib.sha256(url.encode("utf-8", errors="replace")).hexdigest()[:8]
        return f"rpc:{digest}"


class Writer:
    def __init__(
        self,
        out_path: Path,
        done_path: Path,
        *,
        queue_size: int,
        fsync: bool,
        write_errors: bool,
        nonzero_only: bool,
    ):
        self.out_path = out_path
        self.done_path = done_path
        self.fsync = fsync
        self.write_errors = write_errors
        self.nonzero_only = nonzero_only
        self.q: queue.Queue[Optional[List[BalanceResult]]] = queue.Queue(maxsize=queue_size)
        self.thread = threading.Thread(target=self._run, name="result-writer", daemon=False)
        self.error: Optional[BaseException] = None
        self.written_records = 0
        self._lock = threading.Lock()

        out_path.parent.mkdir(parents=True, exist_ok=True)
        done_path.parent.mkdir(parents=True, exist_ok=True)
        self.thread.start()

    def push(self, results: List[BalanceResult], stop_event: threading.Event) -> None:
        while True:
            self.raise_if_failed()
            try:
                self.q.put(results, timeout=0.25)
                return
            except queue.Full:
                if stop_event.is_set() and not self.thread.is_alive():
                    self.raise_if_failed()
                    raise RuntimeError("writer stopped unexpectedly")

    def stop(self) -> None:
        if self.thread.is_alive():
            self.q.put(None)
            self.thread.join()
        self.raise_if_failed()

    def raise_if_failed(self) -> None:
        if self.error is not None:
            raise RuntimeError("writer thread failed") from self.error

    def _run(self) -> None:
        try:
            self._write_loop()
        except BaseException as exc:
            self.error = exc
            logging.exception("Writer thread failed")

    def _write_loop(self) -> None:
        buffered = 0
        last_flush = monotonic()

        with self.out_path.open("a", encoding="utf-8", buffering=1024 * 1024) as out_f, self.done_path.open(
            "a", encoding="utf-8", buffering=1024 * 1024
        ) as done_f:
            while True:
                timeout = max(0.1, WRITER_FLUSH_EVERY_SEC - (monotonic() - last_flush))
                try:
                    batch = self.q.get(timeout=timeout)
                except queue.Empty:
                    batch = []

                if batch is None:
                    break

                for result in batch:
                    should_write_output = (
                        (result.ok and (not self.nonzero_only or (result.balance_wei or 0) > 0))
                        or (not result.ok and self.write_errors)
                    )
                    if should_write_output:
                        out_f.write(json.dumps(result.dump(), ensure_ascii=False, separators=(",", ":")) + "\n")

                    # Mark success as done even when a zero balance is intentionally omitted.
                    # Output is flushed before the done index, so a crash cannot mark a record
                    # completed before its result is durable.
                    if result.ok:
                        done_f.write(result.address + "\n")
                    buffered += 1

                now = monotonic()
                if buffered >= WRITER_FLUSH_EVERY_LINES or now - last_flush >= WRITER_FLUSH_EVERY_SEC:
                    self._flush(out_f, done_f)
                    buffered = 0
                    last_flush = now

                if batch:
                    with self._lock:
                        self.written_records += len(batch)

            self._flush(out_f, done_f)

    def _flush(self, out_f, done_f) -> None:
        out_f.flush()
        if self.fsync:
            os.fsync(out_f.fileno())
        done_f.flush()
        if self.fsync:
            os.fsync(done_f.fileno())


class Stats:
    def __init__(self, total: int):
        self.total = total
        self.ok = 0
        self.errors = 0
        self._lock = threading.Lock()
        self.started_at = monotonic()

    def add(self, results: Sequence[BalanceResult]) -> None:
        ok = sum(1 for result in results if result.ok)
        with self._lock:
            self.ok += ok
            self.errors += len(results) - ok

    def snapshot(self) -> Tuple[int, int, int, float, Optional[float]]:
        with self._lock:
            done = self.ok + self.errors
            elapsed = max(0.001, monotonic() - self.started_at)
            speed = done / elapsed
            eta = (self.total - done) / speed if speed > 0 else None
            return done, self.ok, self.errors, speed, eta


def parse_rpc_item(item: object, address: str) -> BalanceResult:
    if not isinstance(item, dict):
        return BalanceResult(address, None, None, "INVALID_RPC_ITEM", 0.0)

    if "error" in item:
        error = item.get("error")
        if isinstance(error, dict):
            code = error.get("code")
            message = str(error.get("message", "RPC_ERROR"))[:300]
            return BalanceResult(address, None, None, f"RPC_ERROR[{code}]: {message}", 0.0)
        return BalanceResult(address, None, None, f"RPC_ERROR: {str(error)[:300]}", 0.0)

    raw = item.get("result")
    if not isinstance(raw, str) or not raw.startswith("0x"):
        return BalanceResult(address, None, None, "BAD_RPC_RESULT", 0.0)

    try:
        wei = int(raw, 16)
    except ValueError:
        return BalanceResult(address, None, None, f"BAD_HEX_BALANCE: {raw[:80]}", 0.0)

    return BalanceResult(address, wei, Decimal(wei) / WEI, None, 0.0)


def call_rpc_batch(
    node: str,
    addresses: Sequence[str],
    timeout: Tuple[float, float],
    user_agent: str,
) -> List[BalanceResult]:
    payload = [
        {
            "jsonrpc": "2.0",
            "id": index,
            "method": "eth_getBalance",
            "params": [address, "latest"],
        }
        for index, address in enumerate(addresses)
    ]

    response = get_session(user_agent).post(node, json=payload, timeout=timeout)
    response.raise_for_status()

    try:
        data = response.json()
    except (ValueError, json.JSONDecodeError) as exc:
        snippet = response.text[:200].replace("\n", " ")
        raise BatchTransportError(f"BAD_JSON: {snippet}") from exc

    if isinstance(data, dict):
        error = data.get("error")
        if isinstance(error, dict):
            raise BatchTransportError(
                f"RPC_BATCH_ERROR[{error.get('code')}]: {str(error.get('message', ''))[:200]}"
            )
        raise BatchTransportError("UNEXPECTED_JSON_OBJECT")
    if not isinstance(data, list):
        raise BatchTransportError("UNEXPECTED_JSON_TYPE")

    mapping: Dict[int, object] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        if isinstance(item_id, int) and 0 <= item_id < len(addresses):
            mapping[item_id] = item

    return [
        parse_rpc_item(mapping.get(index), address)
        if index in mapping
        else BalanceResult(address, None, None, "MISSING_RPC_RESPONSE", 0.0)
        for index, address in enumerate(addresses)
    ]


def fetch_batch(
    node_mgr: NodeManager,
    addresses: List[str],
    max_retries: int,
    timeout: Tuple[float, float],
    stop_event: threading.Event,
    user_agent: str,
) -> List[BalanceResult]:
    pending = list(addresses)
    completed: Dict[str, BalanceResult] = {}
    tried_nodes: Set[str] = set()
    last_error = "UNKNOWN"

    for attempt in range(1, max_retries + 1):
        if not pending or stop_event.is_set():
            break
        if len(tried_nodes) >= len(node_mgr.nodes):
            tried_nodes.clear()

        try:
            node = node_mgr.acquire_node(tried_nodes, stop_event)
        except NodeUnavailable as exc:
            last_error = str(exc)
            break

        tried_nodes.add(node)
        started = monotonic()
        node_ok = False
        rate_limited = False
        server_error = False

        try:
            results = call_rpc_batch(node, pending, timeout, user_agent)
            latency_ms = (monotonic() - started) * 1000.0

            retry_addresses: List[str] = []
            for result in results:
                result.latency_ms = latency_ms
                result.node = mask_url(node)
                result.attempts = attempt
                if result.ok:
                    completed[result.address] = result
                else:
                    retry_addresses.append(result.address)
                    last_error = result.error or "RPC_ITEM_ERROR"

            # The transport worked. A node with partial item errors is only mildly penalized
            # by retrying those items elsewhere; it is not circuit-broken as a dead endpoint.
            node_ok = True
            pending = retry_addresses

        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            rate_limited = status == 429
            server_error = status is not None and 500 <= status <= 599
            last_error = f"HTTP_{status}" if status else "HTTP_ERROR"
        except (requests.Timeout, requests.ConnectionError) as exc:
            server_error = True
            last_error = type(exc).__name__
        except BatchTransportError as exc:
            last_error = str(exc)[:300]
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {str(exc)[:240]}"
        finally:
            latency_ms = (monotonic() - started) * 1000.0
            node_mgr.release_node(
                node,
                success=node_ok,
                latency_ms=latency_ms,
                rate_limited=rate_limited,
                server_error=server_error,
            )

        if pending and attempt < max_retries and not stop_event.is_set():
            stop_event.wait(min(3.0, 0.15 * (2 ** (attempt - 1)) + random.uniform(0.05, 0.35)))

    for address in pending:
        completed[address] = BalanceResult(
            address=address,
            balance_wei=None,
            balance_eth=None,
            error=f"FAILED_AFTER_{max_retries}_ATTEMPTS: {last_error}",
            latency_ms=0.0,
            node=None,
            attempts=max_retries,
        )

    # Preserve original input order.
    return [completed[address] for address in addresses if address in completed]


def iter_input_addresses(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
        for line_number, line in enumerate(handle, start=1):
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            candidate = raw.split(",", 1)[0].split(";", 1)[0].strip()
            if not Web3.is_address(candidate):
                logging.debug("Skipping invalid address at line %d: %r", line_number, candidate)
                continue
            yield Web3.to_checksum_address(candidate)


def load_addresses(path: Path) -> List[str]:
    seen: Set[str] = set()
    addresses: List[str] = []
    for address in iter_input_addresses(path):
        if address not in seen:
            seen.add(address)
            addresses.append(address)
    return addresses


def load_done(out_path: Path, done_path: Path) -> Set[str]:
    done: Set[str] = set()
    source = done_path if done_path.exists() else out_path
    if not source.exists():
        return done

    with source.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                if source == done_path:
                    address = line.strip()
                    success = True
                else:
                    obj = json.loads(line)
                    address = obj.get("address")
                    success = obj.get("error") is None
                if success and address and Web3.is_address(address):
                    done.add(Web3.to_checksum_address(address))
            except (ValueError, TypeError, json.JSONDecodeError):
                continue
    return done


def parse_nodes(raw: str) -> List[str]:
    nodes: List[str] = []
    for item in raw.replace("\n", ",").split(","):
        node = item.strip()
        if not node:
            continue
        parts = urlsplit(node)
        if parts.scheme not in {"http", "https"} or not parts.netloc:
            raise ValueError(f"Invalid RPC URL: {mask_url(node)}")
        nodes.append(node)
    return list(dict.fromkeys(nodes))


def batched(items: Sequence[str], size: int) -> Iterator[List[str]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def format_duration(seconds: Optional[float]) -> str:
    if seconds is None or seconds < 0:
        return "?"
    seconds_i = int(seconds)
    hours, rem = divmod(seconds_i, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:d}:{minutes:02d}:{secs:02d}" if hours else f"{minutes:02d}:{secs:02d}"


def validate_args(args: argparse.Namespace, node_count: int) -> None:
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    if args.batch < 1:
        raise SystemExit("--batch must be >= 1")
    if args.max_inflight < 1:
        raise SystemExit("--max-inflight must be >= 1")
    if args.retries < 1:
        raise SystemExit("--retries must be >= 1")
    if args.node_concurrency < 1:
        raise SystemExit("--node-concurrency must be >= 1")
    if args.connect_timeout <= 0 or args.read_timeout <= 0:
        raise SystemExit("timeouts must be > 0")
    if args.max_inflight < args.workers:
        logging.warning("--max-inflight is below --workers; effective parallelism will be limited")
    theoretical = node_count * args.node_concurrency
    if args.workers > theoretical:
        logging.warning(
            "workers=%d exceeds total per-node capacity=%d; extra threads may mostly wait",
            args.workers,
            theoretical,
        )


def run(
    *,
    nodes: List[str],
    addresses: List[str],
    workers: int,
    batch_size: int,
    max_inflight: int,
    max_retries: int,
    node_concurrency: int,
    out_path: Path,
    done_path: Path,
    timeout: Tuple[float, float],
    fsync: bool,
    write_errors: bool,
    nonzero_only: bool,
    user_agent: str,
) -> int:
    stop_event = threading.Event()
    node_mgr = NodeManager(nodes, per_node_limit=node_concurrency)

    already_done = load_done(out_path, done_path)
    remaining = [address for address in addresses if address not in already_done]
    total = len(remaining)
    stats = Stats(total)
    writer = Writer(
        out_path,
        done_path,
        queue_size=max(16, max_inflight * 2),
        fsync=fsync,
        write_errors=write_errors,
        nonzero_only=nonzero_only,
    )

    logging.info("RPC nodes: %d", len(nodes))
    logging.info("Input unique wallets: %d | already done: %d | remaining: %d", len(addresses), len(already_done), total)
    logging.info(
        "Workers: %d | batch: %d | max_inflight: %d | per-node limit: %d",
        workers,
        batch_size,
        max_inflight,
        node_concurrency,
    )

    if total == 0:
        writer.stop()
        logging.info("Nothing to do")
        return 0

    def request_stop(signum=None, frame=None) -> None:  # noqa: ARG001
        if not stop_event.is_set():
            logging.warning("Shutdown requested; no new batches will be submitted")
            stop_event.set()
            node_mgr.wake_all()

    previous_sigint = signal.signal(signal.SIGINT, request_stop)
    previous_sigterm = signal.signal(signal.SIGTERM, request_stop)

    batches = iter(batched(remaining, batch_size))
    futures: Set[Future[List[BalanceResult]]] = set()
    exhausted = False
    last_progress = monotonic()
    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="rpc")

    try:
        while (not exhausted and not stop_event.is_set()) or futures:
            while not exhausted and not stop_event.is_set() and len(futures) < max_inflight:
                try:
                    batch = next(batches)
                except StopIteration:
                    exhausted = True
                    break
                futures.add(
                    executor.submit(
                        fetch_batch,
                        node_mgr,
                        batch,
                        max_retries,
                        timeout,
                        stop_event,
                        user_agent,
                    )
                )

            if not futures:
                break

            completed, futures = wait(futures, timeout=0.5, return_when=FIRST_COMPLETED)
            for future in completed:
                try:
                    results = future.result()
                except Exception:
                    logging.exception("Unexpected worker failure")
                    stop_event.set()
                    node_mgr.wake_all()
                    continue
                stats.add(results)
                writer.push(results, stop_event)

            writer.raise_if_failed()
            now = monotonic()
            if now - last_progress >= PROGRESS_EVERY_SEC:
                done, ok, errors, speed, eta = stats.snapshot()
                logging.info(
                    "Progress: %d/%d | ok=%d | err=%d | %.1f wallets/s | ETA %s",
                    done,
                    total,
                    ok,
                    errors,
                    speed,
                    format_duration(eta),
                )
                logging.debug("Node stats: %s", json.dumps(node_mgr.snapshot(), ensure_ascii=False))
                last_progress = now

    finally:
        stop_event.set()
        node_mgr.wake_all()
        for future in futures:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
        writer.stop()
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)

    done, ok, errors, speed, _ = stats.snapshot()
    logging.info(
        "Finished: %d/%d | ok=%d | err=%d | average %.1f wallets/s",
        done,
        total,
        ok,
        errors,
        speed,
    )
    logging.info("Node stats: %s", json.dumps(node_mgr.snapshot(), ensure_ascii=False))

    if stop_event.is_set() and done < total:
        logging.warning("Stopped early. Resume by running the same command again.")
        return 130
    return 1 if errors else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ethereum wallet balance checker with multi-RPC batching and durable resume",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-i", "--input", default="wallets.txt", help="Input wallet file")
    parser.add_argument("-o", "--output", default="balances.jsonl", help="Output JSONL file")
    parser.add_argument(
        "-n",
        "--nodes",
        default=os.getenv("RPC_URLS", ""),
        help="Comma/newline-separated RPC URLs; defaults to RPC_URLS env var",
    )
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-inflight", type=int, default=DEFAULT_MAX_INFLIGHT)
    parser.add_argument("--retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--node-concurrency", type=int, default=DEFAULT_NODE_CONCURRENCY)
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT)
    parser.add_argument("--read-timeout", type=float, default=DEFAULT_READ_TIMEOUT)
    parser.add_argument("--done-index", default=None, help="Resume index; default is OUTPUT.done")
    parser.add_argument("--fsync", action="store_true", help="fsync output/index after each writer flush")
    parser.add_argument("--no-error-output", action="store_true", help="Do not write failed records to JSONL")
    parser.add_argument("--nonzero-only", action="store_true", help="Write only successful non-zero balances")
    parser.add_argument("--user-agent", default="eth-balance-checker/2.0")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
    )

    try:
        nodes = parse_nodes(args.nodes)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if not nodes:
        raise SystemExit("No RPC nodes provided. Use --nodes or RPC_URLS.")

    validate_args(args, len(nodes))

    input_path = Path(args.input)
    output_path = Path(args.output)
    done_path = Path(args.done_index) if args.done_index else Path(f"{output_path}.done")

    if not input_path.is_file():
        raise SystemExit(f"Input file not found: {input_path}")
    if output_path.resolve() == done_path.resolve():
        raise SystemExit("Output file and done index must be different files")

    addresses = load_addresses(input_path)
    if not addresses:
        raise SystemExit("No valid Ethereum addresses found")

    return run(
        nodes=nodes,
        addresses=addresses,
        workers=args.workers,
        batch_size=args.batch,
        max_inflight=args.max_inflight,
        max_retries=args.retries,
        node_concurrency=args.node_concurrency,
        out_path=output_path,
        done_path=done_path,
        timeout=(args.connect_timeout, args.read_timeout),
        fsync=args.fsync,
        write_errors=not args.no_error_output,
        nonzero_only=args.nonzero_only,
        user_agent=args.user_agent,
    )


if __name__ == "__main__":
    raise SystemExit(main())
