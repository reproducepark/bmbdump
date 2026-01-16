#!/usr/bin/env python3
"""
Download blocks in parallel from btc-rpc-explorer style API.

- Range: [start_height, end_height] inclusive
- Parallelism: default 10
- Progress: tqdm
- Resume: progress.csv checkpoint + file existence check
- Storage: gzip-compressed JSON per block to reduce bytes
"""

import argparse
import asyncio
import csv
import gzip
import json
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

import aiohttp
from tqdm import tqdm


DEFAULT_BASE_URL = "https://blockchain.mobick.info"
# Default output directory (relative to current working directory).
# Use a relative path so it behaves nicely on Windows/macOS/Linux.
DEFAULT_OUT_DIR = "dump"
DEFAULT_CONCURRENCY = 100
DEFAULT_TIMEOUT_SEC = 30
DEFAULT_RETRIES = 8


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def block_url(base_url: str, height: int) -> str:
    return f"{base_url.rstrip('/')}/api/block/{height}"


def out_path_for_height(out_dir: Path, height: int) -> Path:
    # Distribute into subdirs by 1000 to avoid too many files in one directory.
    subdir = out_dir / "blocks" / f"{height // 1000:06d}"
    subdir.mkdir(parents=True, exist_ok=True)
    return subdir / f"{height}.json.gz"


@dataclass
class CheckpointRow:
    height: int
    status: str  # "ok" or "fail"
    path: str
    tries: int
    http_status: str
    error: str
    updated_at_utc: str


class Checkpoint:
    """
    CSV checkpoint writer/reader.
    Stores one row per completion (ok/fail). On resume, we treat the latest status for each height as authoritative.
    """
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self._lock = asyncio.Lock()
        self._latest: Dict[int, CheckpointRow] = {}

    def load(self) -> None:
        if not self.csv_path.exists():
            return
        try:
            with self.csv_path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    try:
                        h = int(r.get("height", "").strip())
                    except Exception:
                        continue
                    row = CheckpointRow(
                        height=h,
                        status=r.get("status", "").strip(),
                        path=r.get("path", "").strip(),
                        tries=int(r.get("tries", "0").strip() or 0),
                        http_status=r.get("http_status", "").strip(),
                        error=r.get("error", "").strip(),
                        updated_at_utc=r.get("updated_at_utc", "").strip(),
                    )
                    self._latest[h] = row
        except Exception as e:
            print(f"[WARN] 체크포인트 CSV 로드 실패: {self.csv_path} ({e})", file=sys.stderr)

    def latest(self, height: int) -> Optional[CheckpointRow]:
        return self._latest.get(height)

    async def append(self, row: CheckpointRow) -> None:
        async with self._lock:
            file_exists = self.csv_path.exists()
            self.csv_path.parent.mkdir(parents=True, exist_ok=True)
            with self.csv_path.open("a", newline="", encoding="utf-8") as f:
                fieldnames = ["height", "status", "path", "tries", "http_status", "error", "updated_at_utc"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow({
                    "height": row.height,
                    "status": row.status,
                    "path": row.path,
                    "tries": row.tries,
                    "http_status": row.http_status,
                    "error": row.error,
                    "updated_at_utc": row.updated_at_utc,
                })
            self._latest[row.height] = row


def is_valid_gz_json(path: Path) -> bool:
    """
    Minimal sanity check: can we open gzip and parse JSON?
    (Avoids re-downloading corrupted partial files.)
    """
    if not path.exists():
        return False
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            json.load(f)
        return True
    except Exception:
        return False


async def fetch_block_json(
    session: aiohttp.ClientSession,
    url: str,
    timeout_sec: int,
) -> Tuple[Optional[dict], Optional[int], Optional[str]]:
    """
    Returns: (json_obj or None, http_status or None, error_message or None)
    """
    try:
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with session.get(url, timeout=timeout, headers={"Accept": "application/json"}) as resp:
            status = resp.status
            if status != 200:
                # read limited text for error
                text = await resp.text()
                text = (text[:500] + "…") if len(text) > 500 else text
                return None, status, f"HTTP {status}: {text}"
            data = await resp.json(content_type=None)
            return data, status, None
    except asyncio.CancelledError:
        raise
    except Exception as e:
        return None, None, str(e)


async def write_gz_json_atomic(path: Path, obj: dict, compresslevel: int = 6) -> None:
    tmp = path.with_suffix(path.suffix + ".part")
    # Write atomically: write to .part then rename
    with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=compresslevel) as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))  # compact JSON
    os.replace(tmp, path)


async def worker(
    name: str,
    queue: "asyncio.Queue[int]",
    session: aiohttp.ClientSession,
    base_url: str,
    out_dir: Path,
    checkpoint: Checkpoint,
    pbar: tqdm,
    timeout_sec: int,
    retries: int,
    stop_event: asyncio.Event,
):
    while not stop_event.is_set():
        try:
            height = await queue.get()
        except asyncio.CancelledError:
            break

        try:
            out_path = out_path_for_height(out_dir, height)

            # Resume skip rule: if checkpoint says ok AND file seems valid -> skip
            latest = checkpoint.latest(height)
            if latest and latest.status == "ok" and is_valid_gz_json(Path(latest.path)):
                continue
            # Also skip if file exists and valid (even if checkpoint missing)
            if is_valid_gz_json(out_path):
                await checkpoint.append(CheckpointRow(
                    height=height, status="ok", path=str(out_path), tries=0,
                    http_status="200", error="", updated_at_utc=utc_now_iso()
                ))
                continue

            url = block_url(base_url, height)

            ok = False
            last_http = ""
            last_err = ""
            for attempt in range(1, retries + 1):
                if stop_event.is_set():
                    break

                data, http_status, err = await fetch_block_json(session, url, timeout_sec=timeout_sec)
                if data is not None:
                    try:
                        # sanity: match height if present
                        if isinstance(data, dict) and "height" in data and int(data["height"]) != height:
                            raise ValueError(f"height mismatch: got {data.get('height')} expected {height}")

                        await write_gz_json_atomic(out_path, data, compresslevel=6)
                        await checkpoint.append(CheckpointRow(
                            height=height,
                            status="ok",
                            path=str(out_path),
                            tries=attempt,
                            http_status=str(http_status or "200"),
                            error="",
                            updated_at_utc=utc_now_iso(),
                        ))
                        ok = True
                        break
                    except Exception as e:
                        last_http = str(http_status or "")
                        last_err = f"write/validate error: {e}"

                else:
                    last_http = str(http_status or "")
                    last_err = err or "unknown error"

                # Backoff: exponential + jitter
                backoff = min(60.0, (2 ** (attempt - 1)) * 0.5) + random.random() * 0.5
                await asyncio.sleep(backoff)

            if not ok and not stop_event.is_set():
                await checkpoint.append(CheckpointRow(
                    height=height,
                    status="fail",
                    path=str(out_path),
                    tries=retries,
                    http_status=last_http,
                    error=last_err,
                    updated_at_utc=utc_now_iso(),
                ))
        finally:
            queue.task_done()
            pbar.update(1)


async def main_async(args: argparse.Namespace) -> int:
    base_url = args.base_url
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_path = Path(args.checkpoint) if args.checkpoint else (out_dir / "progress.csv")
    checkpoint = Checkpoint(checkpoint_path)
    checkpoint.load()

    start_h = args.start
    end_h = args.end
    if start_h > end_h:
        print("[ERROR] start height가 end height보다 큽니다.", file=sys.stderr)
        return 2

    heights = list(range(start_h, end_h + 1))
    total = len(heights)

    # Create work queue
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=args.concurrency * 4)

    # Pre-fill queue in producer task to allow graceful stopping
    stop_event = asyncio.Event()

    def _handle_sigint(*_):
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_sigint)
        except NotImplementedError:
            # Windows fallback
            signal.signal(sig, lambda *_: _handle_sigint())

    # Producer: enqueue all heights (but allow stop)
    async def producer():
        for h in heights:
            if stop_event.is_set():
                break
            await queue.put(h)

    timeout = aiohttp.ClientTimeout(total=None)
    connector = aiohttp.TCPConnector(limit=args.concurrency, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        pbar = tqdm(total=total, desc="Downloading blocks", unit="block")

        prod_task = asyncio.create_task(producer())

        workers = [
            asyncio.create_task(
                worker(
                    name=f"w{i}",
                    queue=queue,
                    session=session,
                    base_url=base_url,
                    out_dir=out_dir,
                    checkpoint=checkpoint,
                    pbar=pbar,
                    timeout_sec=args.timeout,
                    retries=args.retries,
                    stop_event=stop_event,
                )
            )
            for i in range(args.concurrency)
        ]

        try:
            # Wait until producer finishes enqueuing then queue drains
            await prod_task
            await queue.join()
        except asyncio.CancelledError:
            stop_event.set()
        finally:
            stop_event.set()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            pbar.close()

    if stop_event.is_set():
        print("\n[INFO] 중단 신호를 감지했습니다. 다음 실행 시 체크포인트를 읽어서 이어받습니다.")
        return 130

    print("\n[INFO] 완료!")
    print(f"- 저장 경로: {out_dir / 'blocks'}")
    print(f"- 체크포인트: {checkpoint_path}")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base url (e.g. https://blockchain.mobick.info)")
    p.add_argument("--start", type=int, default=556760, help="start height (inclusive)")
    p.add_argument("--end", type=int, default=855698, help="end height (inclusive)")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="output base directory (default: ./dump)")
    p.add_argument("--checkpoint", default=None, help="checkpoint CSV path (default: <out-dir>/progress.csv)")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="parallel requests (default: 10)")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SEC, help="HTTP timeout seconds (default: 30)")
    p.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="max retries per block (default: 8)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    try:
        rc = asyncio.run(main_async(args))
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()
