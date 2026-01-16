#!/usr/bin/env python3
"""
dump/txs/**/{height}.jsonl 파일들을 하나의 JSONL로 합칩니다. (blocks.jsonl처럼)

- 입력(기본): dump/blocks.jsonl (블록 높이 순서 기준으로 tx 파일을 순차 결합)
- 입력 tx: dump/txs/000556/556760.jsonl 형태
- 출력: dump/txs.jsonl (한 줄에 tx JSON 1개)
- 진행률: tqdm (block 단위)

예시:
  python txs_to_jsonl.py --blocks-jsonl dump/blocks.jsonl --dump-dir dump --out dump/txs.jsonl
  python txs_to_jsonl.py --blocks-jsonl dump/_test_blocks2.jsonl --dump-dir dump --out dump/_test_txs.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, Tuple

from tqdm import tqdm


def count_lines(path: Path) -> int:
    with path.open("rb") as f:
        return sum(1 for _ in f)


def tx_path_for_height(dump_dir: Path, height: int) -> Path:
    return dump_dir / "txs" / f"{height // 1000:06d}" / f"{height}.jsonl"


def iter_heights_from_blocks_jsonl(blocks_jsonl: Path) -> Iterator[int]:
    with blocks_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            b = json.loads(line)
            yield int(b["height"])


def write_jsonl_atomic(out_path: Path, writer_fn) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    if tmp.exists():
        try:
            tmp.unlink()
        except Exception:
            pass
    writer_fn(tmp)
    os.replace(tmp, out_path)


def merge_txs(
    *,
    blocks_jsonl: Path,
    dump_dir: Path,
    out_path: Path,
    strict: bool,
    errors_path: Path | None,
    no_count: bool,
) -> int:
    total_blocks = None if no_count else count_lines(blocks_jsonl)

    err_f = None
    if errors_path is not None:
        errors_path.parent.mkdir(parents=True, exist_ok=True)
        err_f = errors_path.open("w", encoding="utf-8", newline="\n")

    stats = {"blocks_ok": 0, "blocks_fail": 0, "tx_written": 0}

    def _write(tmp_out: Path) -> None:
        with tmp_out.open("w", encoding="utf-8", newline="\n") as out_f:
            for h in tqdm(iter_heights_from_blocks_jsonl(blocks_jsonl), total=total_blocks, desc="Merging txs", unit="block"):
                txp = tx_path_for_height(dump_dir, h)
                if not txp.exists():
                    stats["blocks_fail"] += 1
                    if err_f is not None:
                        err_f.write(
                            json.dumps(
                                {"height": h, "path": str(txp), "error": "missing tx file"},
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                            + "\n"
                        )
                    if strict:
                        raise FileNotFoundError(str(txp))
                    continue

                try:
                    # 블록 내부 tx 순서를 유지하기 위해 파일 라인 순서대로 그대로 처리
                    with txp.open("r", encoding="utf-8") as tx_f:
                        for line in tx_f:
                            s = line.strip()
                            if not s:
                                continue
                            # JSON 유효성 체크 + 한 줄 JSON으로 통일
                            obj = json.loads(s)
                            out_f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
                            stats["tx_written"] += 1
                    stats["blocks_ok"] += 1
                except Exception as e:
                    stats["blocks_fail"] += 1
                    if err_f is not None:
                        err_f.write(
                            json.dumps(
                                {"height": h, "path": str(txp), "error": str(e)},
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                            + "\n"
                        )
                    if strict:
                        raise

    try:
        write_jsonl_atomic(out_path, _write)
    finally:
        if err_f is not None:
            err_f.close()

    print("[INFO] done")
    print(f"- blocks_ok: {stats['blocks_ok']}")
    print(f"- blocks_fail: {stats['blocks_fail']}")
    print(f"- tx_written: {stats['tx_written']}")
    print(f"- out: {out_path}")
    if errors_path is not None:
        print(f"- errors: {errors_path}")

    return 0 if stats["blocks_fail"] == 0 else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dump-dir", default="dump", help="dump 디렉토리 (기본: dump)")
    p.add_argument("--blocks-jsonl", default="dump/blocks.jsonl", help="블록 순서 기준 blocks.jsonl 경로")
    p.add_argument("--out", default="dump/txs.jsonl", help="출력 txs jsonl 경로")
    p.add_argument("--strict", action="store_true", help="tx 파일 누락/파싱 에러가 있으면 즉시 중단")
    p.add_argument("--no-count", action="store_true", help="tqdm total(라인 카운트) 생략")
    p.add_argument(
        "--errors",
        default="dump/txs_to_jsonl_errors.jsonl",
        help="에러 로그 jsonl 경로 (기본: dump/txs_to_jsonl_errors.jsonl). 비우려면 --errors ''",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    dump_dir = Path(args.dump_dir)
    blocks_jsonl = Path(args.blocks_jsonl)
    out_path = Path(args.out)

    if not blocks_jsonl.exists():
        print(f"[ERROR] blocks.jsonl not found: {blocks_jsonl}", file=sys.stderr)
        return 2

    errors_path = None
    if isinstance(args.errors, str) and args.errors.strip() != "":
        errors_path = Path(args.errors)

    return merge_txs(
        blocks_jsonl=blocks_jsonl,
        dump_dir=dump_dir,
        out_path=out_path,
        strict=args.strict,
        errors_path=errors_path,
        no_count=args.no_count,
    )


if __name__ == "__main__":
    raise SystemExit(main())

