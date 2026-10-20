#!/usr/bin/env python3
"""Receiver-capture oracle for UDP duplication and reordering E2E scenarios.

Only Python's standard library is needed; `run` additionally needs tcpdump and
scalpel-run. Capture is inbound on the client's Ethernet veth, AFTER netem.
"""

import argparse
import calendar
from collections import defaultdict
import csv
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import re
import signal
import struct
import subprocess
import time

import verify


def timestamp_ns(value):
    match = re.fullmatch(r"(.+T\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)", value)
    if not match:
        raise ValueError(f"invalid timestamp {value!r}")
    dt = datetime.fromisoformat(match[1] + match[3].replace("Z", "+00:00"))
    return calendar.timegm(dt.utctimetuple()) * 10**9 + int((match[2] or "").ljust(9, "0"))


def read_capture(path):
    """Read nanosecond classic Ethernet pcap from our narrowly filtered tcpdump."""
    packets = []
    with open(path, "rb") as stream:
        header = stream.read(24)
        formats = {b"\x4d\x3c\xb2\xa1": "<", b"\xa1\xb2\x3c\x4d": ">"}
        if len(header) != 24 or header[:4] not in formats:
            raise ValueError("expected nanosecond classic pcap")
        endian = formats[header[:4]]
        if struct.unpack(endian + "I", header[20:24])[0] != 1:
            raise ValueError("expected Ethernet capture")
        while record := stream.read(16):
            if len(record) != 16:
                raise ValueError("truncated pcap record")
            sec, ns, caplen, _ = struct.unpack(endian + "IIII", record)
            frame = stream.read(caplen)
            if len(frame) != caplen or len(frame) < 42 or frame[12:14] != b"\x08\x00":
                raise ValueError("truncated or non-IPv4 capture packet")
            ip = frame[14:]
            ihl = (ip[0] & 15) * 4
            if ip[0] >> 4 != 4 or ip[9] != 17 or int.from_bytes(ip[6:8], "big") & 0x3FFF:
                raise ValueError("expected unfragmented IPv4 UDP")
            udp = ip[ihl:]
            if ihl < 20 or len(udp) < 16:
                raise ValueError("missing UDP sequence in capture")
            size = int.from_bytes(udp[4:6], "big") - 8
            if size < 8:
                raise ValueError("invalid UDP payload size")
            packets.append((int.from_bytes(udp[8:16], "big"), sec * 10**9 + ns, size))
    return packets


def check(csv_path, pcap, capture_log, scenario, min_packets=250):
    errors = []
    parsed = verify.parse_rows(csv_path, errors)
    verify.check_common(csv_path, parsed, min_packets, errors)
    if errors:
        return errors
    log = Path(capture_log).read_text()
    drops = re.findall(r"(\d+) packets dropped by kernel", log)
    if not drops or int(drops[-1]) != 0:
        errors.append("capture drop statistics must be present and zero")
    packets = read_capture(pcap)
    arrivals = defaultdict(list)
    for seq, ts, size in packets:
        arrivals[seq].append((ts, size))
    with open(csv_path, newline="") as stream:
        rows = list(csv.DictReader(stream))
    if [int(r["seq"]) for r in rows] != list(range(len(rows))):
        errors.append("CSV must contain exactly one row per sequence, starting at zero")
    for r in rows:
        seq = int(r["seq"])
        copies = sorted(arrivals.pop(seq, []))
        if not copies:
            errors.append(f"seq {seq}: no captured arrival (scenario expects no loss)")
            continue
        if r["lost"] != "false":
            errors.append(f"seq {seq}: captured packet marked lost")
            continue
        actual = [timestamp_ns(r["ts_rcvd"])] + [
            timestamp_ns(r[f"ts_rcvd_{i}"]) for i in range(1, int(r["dup_count"]) + 1)
        ]
        expected = [ts for ts, _ in copies]
        # Both are the same ingress skb software timestamp, not userspace
        # observation times. Require exact nanosecond equality, even for ties.
        if actual != expected:
            errors.append(f"seq {seq}: CSV arrivals {actual} != capture {expected}")
        if any(size != int(r["size"]) for _, size in copies):
            errors.append(f"seq {seq}: payload size differs from capture")
        expected_owd = expected[0] - timestamp_ns(r["ts_sent"])
        if abs(Decimal(r["owd_ms"]) * 10**6 - expected_owd) > Decimal("0.001"):
            errors.append(f"seq {seq}: OWD does not use earliest captured arrival")
        expected_count = 2 if scenario == "duplicates" else 1
        if len(copies) != expected_count:
            errors.append(f"seq {seq}: captured {len(copies)} copies, expected {expected_count}")
    if arrivals:
        errors.append(f"capture contains sequences absent from CSV: {sorted(arrivals)}")
    # Use first arrivals only: repeated copies are not evidence of reordering.
    first = {}
    for seq, ts, _ in packets:
        first[seq] = min(first.get(seq, ts), ts)
    inversions = sum(first[a] > first[b] for a, b in zip(sorted(first), sorted(first)[1:]))
    if scenario == "reordering" and inversions == 0:
        errors.append("capture shows no first-arrival reordering; impairment was not exercised")
    print(f"{scenario}: sent={len(rows)} captured={len(packets)} first-arrival inversions={inversions}")
    return errors


def run(ip, interface):
    # Exclude outgoing client probes and empty server hole-punch replies.
    # Measurement payloads contain at least the eight-byte sequence number.
    with open("capture.log", "w") as log:
        capture = subprocess.Popen([
            "tcpdump", "-i", interface, "-Q", "in", "-nn", "-U", "-s", "128",
            "--time-stamp-precision=nano", "-w", "delivery.pcap",
            f"udp and src host {ip} and udp[4:2] >= 16",
        ], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 10
            while "listening on" not in Path("capture.log").read_text():
                if capture.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("tcpdump failed to become ready; see capture.log")
                time.sleep(0.01)
            subprocess.run([
                "scalpel-run", "udp-periodic", "--ip", ip, "--direction", "dl",
                "--interval", "5", "--duration", "2000", "--out", "./delivery.csv",
                "--log", "./delivery.log", "--log-level=-4",
            ], check=True, timeout=30)
            time.sleep(0.2)  # flush any delayed final copies before stopping capture
        finally:
            if capture.poll() is None:
                capture.send_signal(signal.SIGINT)
            try:
                code = capture.wait(timeout=10)
            except subprocess.TimeoutExpired:
                capture.kill()
                capture.wait()
                raise RuntimeError("tcpdump failed to stop")
        if code != 0:
            raise RuntimeError(f"tcpdump exited with {code}; see capture.log")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("run")
    p.add_argument("--ip", required=True)
    p.add_argument("--interface", required=True)
    p = sub.add_parser("check")
    p.add_argument("--csv", required=True)
    p.add_argument("--pcap", required=True)
    p.add_argument("--capture-log", required=True)
    p.add_argument("--scenario", choices=["duplicates", "reordering"], required=True)
    p.add_argument("--min-packets", type=int, default=250)
    args = parser.parse_args()
    if args.command == "run":
        run(args.ip, args.interface)
        return 0
    try:
        errors = check(args.csv, args.pcap, args.capture_log, args.scenario, args.min_packets)
    except (ValueError, KeyError, OSError, ArithmeticError) as exc:
        errors = [str(exc)]
    for error in errors[:20]:
        print("FAIL:", error)
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
