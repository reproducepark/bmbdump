#!/usr/bin/env python3
"""
dump/blocks/**/{height}.json.gz 파일들을 하나의 JSONL로 합칩니다.

- 입력: dump/blocks/000556/556760.json.gz 형태 (1000개 단위 폴더)
- 출력: 한 줄에 블록 JSON 1개 (jsonl)
- 진행률: tqdm

예시:
  python blocks_to_jsonl.py --dump-dir dump --out dump/blocks.jsonl
  python blocks_to_jsonl.py --dump-dir dump --start 556760 --end 716759 --out dump/blocks_160k.jsonl
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path
from typing import Iterable, Iterator, Tuple

from tqdm import tqdm


def iter_paths_by_scan(blocks_dir: Path) -> Iterator[Tuple[int, Path]]:
    # blocks_dir 아래의 *.json.gz를 모두 스캔해서 (height, path)로 반환
    # 파일명(확장자 제거)이 정수 height인 것만 처리
    for p in blocks_dir.rglob("*.json.gz"):
        if p.name.endswith(".json.gz.part"):
            continue
        stem = p.name[: -len(".json.gz")]
        try:
            h = int(stem)
        except Exception:
            continue
        yield h, p


def iter_paths_by_range(dump_dir: Path, start: int, end: int) -> Iterator[Tuple[int, Path]]:
    blocks_dir = dump_dir / "blocks"
    for h in range(start, end + 1):
        subdir = blocks_dir / f"{h // 1000:06d}"
        yield h, (subdir / f"{h}.json.gz")


def write_jsonl(
    items: Iterable[Tuple[int, Path]],
    total: int,
    out_path: Path,
    *,
    strict: bool,
    errors_path: Path | None,
) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = out_path.with_suffix(out_path.suffix + ".part")

    err_f = None
    if errors_path is not None:
        errors_path.parent.mkdir(parents=True, exist_ok=True)
        err_f = errors_path.open("w", encoding="utf-8", newline="\n")

    ok = 0
    fail = 0

    try:
        with tmp_out.open("w", encoding="utf-8", newline="\n") as out_f:
            for height, path in tqdm(items, total=total, desc="Building JSONL", unit="block"):
                try:
                    if not path.exists():
                        raise FileNotFoundError(str(path))
                    with gzip.open(path, "rt", encoding="utf-8") as f:
                        obj = json.load(f)

                    # "일관된 jsonl": 한 줄에 JSON 오브젝트 1개 보장
                    if isinstance(obj, dict):
                        if "height" not in obj:
                            obj["height"] = height
                    else:
                        # 혹시 다른 타입이면 wrapper로 통일
                        obj = {"height": height, "data": obj}

                    out_f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
                    ok += 1
                except Exception as e:
                    fail += 1
                    msg = f"[WARN] height={height} path={path} error={e}"
                    print(msg, file=sys.stderr)
                    if err_f is not None:
                        err_f.write(
                            json.dumps(
                                {"height": height, "path": str(path), "error": str(e)},
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                            + "\n"
                        )
                    if strict:
                        raise
    finally:
        if err_f is not None:
            err_f.close()

    # 성공적으로 끝났을 때만 원자적으로 교체
    Path(tmp_out).replace(out_path)
    # Windows 콘솔/로그 인코딩 이슈를 피하려고 INFO 로그는 ASCII로 출력
    print(f"[INFO] done: ok={ok} fail={fail} out={out_path}")
    if errors_path is not None:
        print(f"[INFO] error log: {errors_path}")
    return 0 if fail == 0 else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dump-dir", default="dump", help="dump 디렉토리 (기본: dump)")
    p.add_argument("--out", default="dump/blocks.jsonl", help="출력 jsonl 경로")
    p.add_argument("--start", type=int, default=None, help="(옵션) 시작 height (inclusive)")
    p.add_argument("--end", type=int, default=None, help="(옵션) 끝 height (inclusive)")
    p.add_argument(
        "--strict",
        action="store_true",
        help="파일 누락/파싱 에러가 있으면 즉시 중단",
    )
    p.add_argument(
        "--errors",
        default="dump/blocks_to_jsonl_errors.jsonl",
        help="에러 로그 jsonl 경로 (기본: dump/blocks_to_jsonl_errors.jsonl). 비우려면 --errors ''",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    dump_dir = Path(args.dump_dir)
    blocks_dir = dump_dir / "blocks"

    out_path = Path(args.out)

    errors_path = None
    if isinstance(args.errors, str) and args.errors.strip() != "":
        errors_path = Path(args.errors)

    if args.start is not None or args.end is not None:
        if args.start is None or args.end is None:
            print("[ERROR] --start and --end must be provided together.", file=sys.stderr)
            return 2
        if args.start > args.end:
            print("[ERROR] --start must be <= --end.", file=sys.stderr)
            return 2
        total = (args.end - args.start) + 1
        items = iter_paths_by_range(dump_dir, args.start, args.end)
        return write_jsonl(items, total, out_path, strict=args.strict, errors_path=errors_path)

    # 스캔 모드: 실제로 존재하는 파일 기준
    pairs = list(iter_paths_by_scan(blocks_dir))
    pairs.sort(key=lambda x: x[0])
    return write_jsonl(pairs, len(pairs), out_path, strict=args.strict, errors_path=errors_path)


if __name__ == "__main__":
    raise SystemExit(main())

