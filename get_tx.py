#!/usr/bin/env python3
import argparse
import json
import socket
import sys
from typing import Any, Dict


def _recv_line(sock: socket.socket, timeout: float = 10.0) -> str:
    sock.settimeout(timeout)
    buf = b""
    while not buf.endswith(b"\n"):
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("서버가 연결을 종료했습니다.")
        buf += chunk
        if len(buf) > 10_000_000:
            raise ValueError("응답이 너무 큽니다(>10MB).")
    return buf.decode("utf-8", errors="replace")


def rpc_call(sock: socket.socket, _id: int, method: str, params: list, timeout: float = 10.0) -> Dict[str, Any]:
    req = json.dumps({"id": _id, "method": method, "params": params}) + "\n"
    sock.sendall(req.encode("utf-8"))
    line = _recv_line(sock, timeout=timeout)
    try:
        return json.loads(line)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON 파싱 실패: {e}\n원문: {line}")


def connect_electrum_tcp_only(host: str, port: int, timeout: float = 5.0) -> socket.socket:
    s = socket.create_connection((host, port), timeout=timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def main():
    ap = argparse.ArgumentParser(description="ElectrumX(TCP)에서 txid로 verbose tx(JSON) 또는 raw tx(hex) 가져오기")
    ap.add_argument("txid", help="64-hex txid")
    ap.add_argument("--host", default="wallet.mobick.info")
    ap.add_argument("--port", type=int, default=40008, help="ElectrumX TCP 포트(보통 50001/40001 등)")
    ap.add_argument("--timeout", type=float, default=10.0)

    # 기본 verbose, 원하면 raw
    ap.add_argument("--raw", action="store_true", help="raw tx hex로 출력(기본은 verbose JSON)")
    ap.add_argument("--keep-hex", action="store_true", help="verbose JSON에 포함된 hex 필드를 제거하지 않음")

    args = ap.parse_args()

    txid = args.txid.strip().lower()
    if len(txid) != 64 or any(c not in "0123456789abcdef" for c in txid):
        print("txid 형식이 이상합니다. 64자리 hex인지 확인하세요.", file=sys.stderr)
        sys.exit(2)

    sock = None
    try:
        sock = connect_electrum_tcp_only(args.host, args.port, timeout=5.0)

        # (권장) 서버 버전 핸드셰이크 (서버에 따라 없어도 동작은 하지만 호환성 체크용)
        r0 = rpc_call(sock, 0, "server.version", ["pyclient", "1.4"], timeout=args.timeout)
        if r0.get("error"):
            print(f"[경고] server.version 에러: {r0['error']}", file=sys.stderr)

        # tx 요청
        params = [txid] if args.raw else [txid, True]
        r1 = rpc_call(sock, 1, "blockchain.transaction.get", params, timeout=args.timeout)

        if r1.get("error"):
            print(f"요청 실패: {r1['error']}", file=sys.stderr)
            sys.exit(1)

        result = r1.get("result")

        if args.raw:
            if not isinstance(result, str):
                print("서버가 raw(hex) 대신 dict를 반환했습니다. (--raw 없이 실행해보세요.)", file=sys.stderr)
                print(json.dumps(result, ensure_ascii=False, indent=2))
                sys.exit(1)
            print(result)
        else:
            if not isinstance(result, dict):
                print("서버가 verbose(dict) 대신 raw(hex)를 반환했습니다. (이 서버는 verbose를 지원하지 않을 수 있어요.)", file=sys.stderr)
                print(result)
                sys.exit(1)

            # 그래프 적재용이면 hex는 용량만 커져서 기본 제거
            if not args.keep_hex:
                result.pop("hex", None)

            print(json.dumps(result, ensure_ascii=False, indent=2))

    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
