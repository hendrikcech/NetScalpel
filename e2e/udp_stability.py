#!/usr/bin/env python3
"""Capture-based, informational UDP pacing benchmark (Linux, Python >=3.11).

No third-party Python modules. Capture on the SENDER, not behind a bottleneck.
Classic Ethernet/RAW/Linux cooked IPv4 pcaps from tcpdump are supported.
"""

import argparse
import calendar
import csv
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import time


def timestamp_ns(text):
    # datetime alone silently discards the sub-microsecond part of Go's CSV.
    from datetime import datetime

    match = re.fullmatch(r"(.+T\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)", text)
    if not match:
        raise ValueError(f"invalid timestamp: {text!r}")
    dt = datetime.fromisoformat(match[1] + match[3].replace("Z", "+00:00"))
    seconds = calendar.timegm(dt.utctimetuple())
    return seconds * 10**9 + int((match[2] or "").ljust(9, "0"))


def read_csv(path):
    with open(path, newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("empty CSV")
    result = {}
    for row in rows:
        seq = int(row["seq"])
        if seq in result:
            raise ValueError(f"duplicate CSV sequence {seq}")
        result[seq] = (timestamp_ns(row["ts_sent"]), int(row["size"]))
    if sorted(result) != list(range(len(result))):
        raise ValueError("CSV sequences must be contiguous from zero")
    return result


def read_pcap(path):
    """Return (seq, kernel capture time ns, UDP payload bytes) in file order."""
    formats = {
        b"\xd4\xc3\xb2\xa1": ("<", 1000),
        b"\xa1\xb2\xc3\xd4": (">", 1000),
        b"\x4d\x3c\xb2\xa1": ("<", 1),
        b"\xa1\xb2\x3c\x4d": (">", 1),
    }
    packets = []
    with open(path, "rb") as stream:
        header = stream.read(24)
        if len(header) != 24 or header[:4] not in formats:
            raise ValueError("expected classic pcap (not pcapng)")
        endian, scale = formats[header[:4]]
        link = struct.unpack(endian + "I", header[20:24])[0]
        if link not in (1, 12, 101, 113, 276):
            raise ValueError(f"unsupported pcap link type {link}")
        while record := stream.read(16):
            if len(record) != 16:
                raise ValueError("truncated pcap record")
            sec, fraction, caplen, _ = struct.unpack(endian + "IIII", record)
            data = stream.read(caplen)
            if len(data) != caplen:
                raise ValueError("truncated pcap packet")
            if link == 1:
                offset, protocol = 14, int.from_bytes(data[12:14], "big")
                while protocol in (0x8100, 0x88A8):
                    protocol = int.from_bytes(data[offset + 2 : offset + 4], "big")
                    offset += 4
            elif link == 113:
                offset, protocol = 16, int.from_bytes(data[14:16], "big")
            elif link == 276:
                offset, protocol = 20, int.from_bytes(data[:2], "big")
            else:
                offset, protocol = 0, 0x0800
            if protocol != 0x0800 or len(data) < offset + 20:
                continue
            ip = data[offset:]
            if ip[0] >> 4 != 4 or ip[9] != 17:
                continue
            if int.from_bytes(ip[6:8], "big") & 0x3FFF:
                raise ValueError("fragmented UDP is not supported")
            ihl = (ip[0] & 15) * 4
            udp = ip[ihl:]
            if ihl < 20 or len(udp) < 8:
                raise ValueError("snaplen too short for UDP header")
            size = int.from_bytes(udp[4:6], "big") - 8
            if size < 8:
                continue
            if len(udp) < 16:
                raise ValueError("snaplen too short for UDP sequence")
            packets.append(
                (int.from_bytes(udp[8:16], "big"), sec * 10**9 + fraction * scale, size)
            )
    return packets


def percentile(values, p):
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * p)]


def window_rates(packets, origin, duration_ns, width_ns):
    # Include empty windows, exclude the incomplete final window. [start, end).
    bins = [0] * (duration_ns // width_ns)
    for _, ts, size in packets:
        index = (ts - origin) // width_ns
        if 0 <= index < len(bins):
            bins[index] += size * 8
    return [bits * 1000 / width_ns for bits in bins]  # decimal Mbps


def longest_run(values, predicate):
    best = current = 0
    for value in values:
        current = current + 1 if predicate(value) else 0
        best = max(best, current)
    return best


def describe_rates(rates, goal, tolerance, width_ms):
    low = sum(x < goal * (1 - tolerance) for x in rates)
    high = sum(x > goal * (1 + tolerance) for x in rates)
    return (
        f"n={len(rates)} mean={statistics.mean(rates):.2f} "
        f"min/p01/p50/p99/max={min(rates):.2f}/{percentile(rates, 0.01):.2f}/"
        f"{percentile(rates, 0.5):.2f}/{percentile(rates, 0.99):.2f}/{max(rates):.2f} Mbps "
        f"CV={statistics.pstdev(rates) / statistics.mean(rates) * 100 if any(rates) else 0:.2f}% "
        f"low/high={low}/{high} ({100 * (low + high) / len(rates):.2f}% outliers) "
        f"longest-low/high={longest_run(rates, lambda x: x < goal * (1 - tolerance)) * width_ms:g}/"
        f"{longest_run(rates, lambda x: x > goal * (1 + tolerance)) * width_ms:g} ms"
    )


def analyze(
    pcap,
    csv_path,
    rate,
    duration_ms,
    windows,
    tolerance,
    capture_log=None,
    sender_log=None,
):
    sent = read_csv(csv_path)
    packets = read_pcap(pcap)
    errors = []
    seen = {}
    duplicates = 0
    unexpected = 0
    for seq, ts, size in packets:
        if seq not in sent or sent[seq][1] != size:
            unexpected += 1
            continue
        if seq in seen:
            duplicates += 1
        else:
            seen[seq] = (ts, size)
    missing = len(sent.keys() - seen.keys())
    if missing or duplicates or unexpected:
        errors.append(
            f"capture missing/duplicate/unexpected={missing}/{duplicates}/{unexpected}"
        )
    drops = None
    if capture_log:
        matches = re.findall(
            r"(\d+) packets dropped by kernel", Path(capture_log).read_text()
        )
        if matches:
            drops = int(matches[-1])
        if drops is None or drops:
            errors.append(f"capture kernel drops={drops} (must be known and zero)")
    else:
        errors.append("capture drop statistics not supplied")
    fallback = None
    log = ""
    if sender_log:
        log = Path(sender_log).read_text()
        fallback = any(
            s in log
            for s in (
                "Failed enabling tx timestamping",
                "using timestamps from run function",
            )
        )
    if not seen or 0 not in seen:
        raise ValueError("no matching packet zero in capture")
    actual = [(seq, ts, size) for seq, (ts, size) in seen.items()]
    origin = seen[0][0]
    duration_ns = round(duration_ms * 1e6)
    # The CLI floors packets/s using 1400 bytes, but record the actual size.
    sizes = {size for _, size in sent.values()}
    goal = int(rate * 1e6 / 8 / 1400) * statistics.mean(sizes) * 8 / 1e6
    lines = [
        f"capture={pcap}",
        f"requested={rate:g} Mbps; effective UDP-payload goal={goal:.6f} Mbps; payload bytes={sorted(sizes)}",
        f"duration={duration_ms:g} ms; sent/captured={len(sent)}/{len(seen)}; "
        f"capture drops={drops}; TX fallback={fallback}; validity={'INVALID' if errors else 'VALID'}",
    ]
    lines.extend("WARNING: " + error for error in errors)
    lines.append(
        "Origin: captured seq=0; includes empty bins; complete, non-overlapping windows; "
        f"outlier outside +/-{tolerance * 100:g}% of effective payload goal. Not Ethernet wire rate."
    )
    lines.append(
        "Capture timestamps are kernel packet-tap times, NOT tcpdump read/flush times or hardware wire times."
    )
    for marker in ("Start UL client", "Dynamic interval calculation"):
        match = re.search(r"time=(\S+)[^\n]*msg=\"" + marker + r"\"", log)
        if match:
            delay_ms = (origin - timestamp_ns(match[1])) / 1e6
            lines.append(
                f"First capture after '{marker}' log: {delay_ms:.3f} ms (log precision ~1ms)"
            )
    metrics = {
        "requested_mbps": rate,
        "effective_mbps": goal,
        "valid": not errors,
        "windows": {},
    }
    for width_ms in windows:
        width_ns = round(width_ms * 1e6)
        rates = window_rates(actual, origin, duration_ns, width_ns)
        lines.append(
            f"CAP {width_ms:g}ms " + describe_rates(rates, goal, tolerance, width_ms)
        )
        metrics["windows"][str(width_ms)] = {
            "mean_mbps": statistics.mean(rates),
            "outlier_percent": 100
            * sum(abs(x / goal - 1) > tolerance for x in rates)
            / len(rates),
        }
        packets_per_window = int(rate * 1e6 / 8 / 1400) * width_ms / 1000
        if 1 / packets_per_window > tolerance:
            lines.append(
                f"  QUANTIZATION LIMITED: {packets_per_window:.3f} target packets/window; "
                f"one packet = {100 / packets_per_window:.1f}% of goal. Outliers need not indicate pacing error."
            )
        steady = rates[math.ceil(100 / width_ms) :]
        if steady:
            lines.append(
                "  >=100ms " + describe_rates(steady, goal, tolerance, width_ms)
            )
    ten = window_rates(actual, origin, duration_ns, 10_000_000)
    lines.append(
        "First 120ms CAP (10ms bins, Mbps): " + " ".join(f"{x:.1f}" for x in ten[:12])
    )
    lines.append(
        "Worst 10ms windows (start ms: Mbps): "
        + " ".join(
            f"{i * 10}:{ten[i]:.1f}"
            for i in sorted(
                range(len(ten)), key=lambda i: abs(ten[i] - goal), reverse=True
            )[:8]
        )
    )
    first60 = window_rates(actual, origin, duration_ns, 60_000_000)[0]
    lines.append(
        f"First 60ms mean={first60:.2f} Mbps ({first60 / goal * 100:.2f}% of goal)"
    )
    cumulative = 0
    deficits = []
    for value in ten:
        cumulative += (value / goal - 1) * 10
        deficits.append(cumulative)
    lines.append(
        f"Cumulative delivery error at 10ms boundaries: min={min(deficits):.3f} "
        f"max={max(deficits):.3f} final={deficits[-1]:.3f} ms of target-rate traffic"
    )
    ordered = sorted(ts for ts, _ in seen.values())
    gaps = [(b - a) / 1e6 for a, b in zip(ordered, ordered[1:])]
    if gaps:
        lines.append(
            f"CAP interpacket gaps: p99={percentile(gaps, 0.99):.6f} max={max(gaps):.6f} ms; "
            f">1ms={sum(x > 1 for x in gaps)} (batching expected at sub-ms scales)"
        )
    metrics["max_gap_ms"] = max(gaps, default=0)
    metrics["first60_percent"] = first60 / goal * 100
    delta = [(seen[seq][0] - sent[seq][0]) / 1000 for seq in seen]
    median = statistics.median(delta)
    lines.append(
        f"Matched capture-minus-CSV TX: p01/median/p99/min/max="
        f"{percentile(delta, 0.01):.3f}/{median:.3f}/{percentile(delta, 0.99):.3f}/"
        f"{min(delta):.3f}/{max(delta):.3f} us; max deviation from median="
        f"{max(abs(x - median) for x in delta):.3f} us"
    )
    tx = [(seq, ts, size) for seq, (ts, size) in sent.items()]
    txrates = window_rates(tx, origin, duration_ns, 10_000_000)
    lines.append("CSV TX 10ms " + describe_rates(txrates, goal, tolerance, 10))
    lines.append(
        f"CAP vs CSV TX 10ms max bin difference={max(abs(a - b) for a, b in zip(ten, txrates)):.3f} Mbps"
    )
    lines.append(
        f"CAP packets outside configured duration from seq=0: "
        f"before={sum(ts < origin for _, ts, _ in actual)} after={sum(ts >= origin + duration_ns for _, ts, _ in actual)}; "
        f"capture span={(ordered[-1] - ordered[0]) / 1e6:.3f} ms"
    )
    return "\n".join(lines), not errors, metrics


def summary_table(metrics):
    lines = [
        "Sweep summary (capture outliers outside configured tolerance)",
        "requested run valid   mean(Mbps) first60(%goal) maxgap(ms)   outliers% by window(ms)",
    ]
    for row in metrics:
        windows = row["windows"]
        mean = next(iter(windows.values()))["mean_mbps"]
        outliers = " ".join(
            f"{w}:{m['outlier_percent']:.2f}" for w, m in windows.items()
        )
        lines.append(
            f"{row['requested_mbps']:9g} {row['repeat']:3} {str(row['valid']):5} "
            f"{mean:12.2f} {row['first60_percent']:14.2f} {row['max_gap_ms']:10.3f}   {outliers}"
        )
    return "\n".join(lines)


def stop(process, sig=signal.SIGTERM):
    if process is None or process.poll() is not None:
        return
    process.send_signal(sig)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def wait_for(predicate, process, timeout=10):
    deadline = time.monotonic() + timeout
    while not predicate():
        if process.poll() is not None:
            raise RuntimeError(f"process exited early: {process.args}")
        if time.monotonic() > deadline:
            raise TimeoutError(f"waiting for {process.args}")
        time.sleep(0.01)


def run(args):
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=False)  # Never overwrite an earlier measurement.
    binary = shutil.which(args.binary)
    if not binary:
        raise ValueError(f"binary not found: {args.binary}")
    metadata = {
        "args": vars(args),
        "platform": platform.platform(),
        "affinity": sorted(os.sched_getaffinity(0)),
        "cpuinfo": Path("/proc/cpuinfo").read_text(),
        "proc_stat_before": Path("/proc/stat").read_text(),
        "binary": binary,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open(binary, "rb") as stream:
        metadata["binary_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
    if shutil.which("git"):
        metadata["git_revision"] = subprocess.run(
            ["git", "rev-parse", "HEAD"], text=True, capture_output=True
        ).stdout.strip()
        metadata["git_status"] = subprocess.run(
            ["git", "status", "--short"], text=True, capture_output=True
        ).stdout
    for name in ("GODEBUG", "GOGC", "GOMEMLIMIT", "GOMAXPROCS"):
        metadata[name] = os.environ.get(name)
    cgroup = Path("/sys/fs/cgroup")
    group_path = Path("/proc/self/cgroup").read_text().strip().split("::")[-1]
    group = cgroup / group_path.lstrip("/")
    metadata["cgroup_path"] = group_path
    metadata["cgroups"] = {}
    for parent in [group, *group.parents]:
        if parent != cgroup and cgroup not in parent.parents:
            break
        for name in ("cpu.max", "cpu.stat", "cpu.pressure"):
            path = parent / name
            if path.exists():
                metadata["cgroups"][str(path)] = path.read_text()
    server = load = client = capture = None
    reports = [
        "NetScalpel static UDP pacing benchmark",
        json.dumps(
            {
                k: v
                for k, v in metadata.items()
                if k not in ("cpuinfo", "proc_stat_before")
            },
            indent=2,
        ),
    ]
    valid = True
    metrics = []
    try:
        if args.start_server:
            with open(out / "server.stdout", "w") as stream:
                server = subprocess.Popen(
                    [
                        binary,
                        "server",
                        "--ip",
                        args.ip,
                        "--port",
                        str(args.port),
                        "--log",
                        str(out / "server.log"),
                        "--log-level=-4",
                    ],
                    stdout=stream,
                    stderr=stream,
                )

            def ready():
                try:
                    with socket.create_connection((args.ip, args.port), timeout=0.1):
                        return True
                except OSError:
                    return False

            wait_for(ready, server)
        if args.compete:
            load = subprocess.Popen(
                [
                    "taskset",
                    "-c",
                    str(args.sender_cpu),
                    sys.executable,
                    "-c",
                    "while True: pass",
                ]
            )
        for rate in args.rates:
            for repeat in range(args.repeats):
                prefix = out / f"rate-{rate:g}-run-{repeat + 1}"
                pcap, capture_log, csv_path, sender_log = [
                    Path(str(prefix) + suffix)
                    for suffix in (".pcap", ".capture.log", ".csv", ".sender.log")
                ]
                with open(capture_log, "w") as stream:
                    capture = subprocess.Popen(
                        [
                            args.tcpdump,
                            "-i",
                            args.interface,
                            "-nn",
                            "-s",
                            "128",
                            "-B",
                            "16384",
                            "--time-stamp-precision=nano",
                            "-w",
                            str(pcap),
                            f"udp and dst host {args.ip}",
                        ],
                        stdout=stream,
                        stderr=stream,
                    )
                wait_for(lambda: "listening on" in capture_log.read_text(), capture)
                command = [
                    binary,
                    "udp-rate",
                    "--ip",
                    args.ip,
                    "--port",
                    str(args.port),
                    "--rate",
                    str(rate),
                    "--duration",
                    str(args.duration_ms),
                    "--out",
                    str(csv_path),
                    "--log",
                    str(sender_log),
                    "--log-level=-4",
                ]
                if args.sender_cpu is not None:
                    command = ["taskset", "-c", str(args.sender_cpu)] + command
                env = os.environ.copy()
                if args.gomaxprocs:
                    env["GOMAXPROCS"] = str(args.gomaxprocs)
                with open(str(prefix) + ".stdout", "w") as stream:
                    client = subprocess.Popen(
                        command, stdout=stream, stderr=stream, env=env
                    )
                if args.pause_ms:
                    # Buffered pcap data appears shortly after transmission starts.
                    wait_for(lambda: pcap.exists() and pcap.stat().st_size > 24, client)
                    time.sleep(args.pause_at_ms / 1000)
                    client.send_signal(signal.SIGSTOP)
                    before = time.monotonic_ns()
                    wall_start = time.time_ns()
                    try:
                        time.sleep(args.pause_ms / 1000)
                    finally:
                        client.send_signal(signal.SIGCONT)
                    pause = {
                        "run": str(prefix),
                        "elapsed_ms": (time.monotonic_ns() - before) / 1e6,
                        "wall_start_ns": wall_start,
                        "wall_end_ns": time.time_ns(),
                    }
                    metadata.setdefault("injected_pauses", []).append(pause)
                client.wait(timeout=args.duration_ms / 1000 + 60)
                if client.returncode:
                    raise RuntimeError(f"sender failed: see {prefix}.stdout")
                time.sleep(0.2)  # Allow the packet socket to drain before SIGINT.
                stop(capture, signal.SIGINT)
                if capture.returncode:
                    raise RuntimeError(f"capture failed: see {capture_log}")
                if args.pause_ms:
                    first = next(ts for seq, ts, _ in read_pcap(pcap) if seq == 0)
                    pause["offset_from_seq0_ms"] = (
                        pause["wall_start_ns"] - first
                    ) / 1e6
                    if (
                        not 0
                        <= pause["offset_from_seq0_ms"]
                        < args.duration_ms - args.pause_ms
                    ):
                        raise RuntimeError(
                            f"pause outside transmission interval: {pause}; retain capture but do not use as pause control"
                        )
                report, ok, run_metrics = analyze(
                    pcap,
                    csv_path,
                    rate,
                    args.duration_ms,
                    args.windows_ms,
                    args.tolerance,
                    capture_log,
                    sender_log,
                )
                run_metrics["repeat"] = repeat + 1
                metrics.append(run_metrics)
                valid &= ok
                reports.append(report)
                Path(str(prefix) + ".txt").write_text(report + "\n")
                (out / "report.txt").write_text(
                    summary_table(metrics) + "\n\n" + "\n\n".join(reports) + "\n"
                )
                (out / "summary.json").write_text(json.dumps(metrics, indent=2) + "\n")
                print(report, flush=True)
    finally:
        if client is not None and client.poll() is None:
            client.send_signal(signal.SIGCONT)
        stop(client)
        stop(capture, signal.SIGINT)
        stop(load)
        stop(server)
        metadata["proc_stat_after"] = Path("/proc/stat").read_text()
        metadata["cgroups_after"] = {
            path: Path(path).read_text()
            for path in metadata["cgroups"]
            if Path(path).exists()
        }
        (out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(summary_table(metrics))
    return 0 if valid else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "analyze"):
        p = sub.add_parser(name)
        p.add_argument("--duration-ms", type=int, default=1000)
        p.add_argument(
            "--windows-ms", type=float, nargs="+", default=[1, 5, 10, 20, 60]
        )
        p.add_argument("--tolerance", type=float, default=0.1)
        if name == "analyze":
            p.add_argument("--pcap", required=True)
            p.add_argument("--csv", required=True)
            p.add_argument("--rate", type=float, required=True)
            p.add_argument("--capture-log", required=True)
            p.add_argument("--sender-log")
        else:
            p.add_argument("--binary", default="scalpel-run")
            p.add_argument("--tcpdump", default="tcpdump")
            p.add_argument(
                "--ip",
                default="127.0.0.1",
                help="IPv4 server address; capture must be on sender",
            )
            p.add_argument("--port", type=int, default=8500)
            p.add_argument("--interface", default="lo")
            p.add_argument("--start-server", action="store_true")
            p.add_argument("--out", required=True)
            p.add_argument(
                "--rates", type=float, nargs="+", default=[10, 100, 250, 500, 750, 1000]
            )
            p.add_argument("--repeats", type=int, default=3)
            p.add_argument("--sender-cpu", type=int)
            p.add_argument("--gomaxprocs", type=int)
            p.add_argument(
                "--compete",
                action="store_true",
                help="run one busy competitor on sender CPU",
            )
            p.add_argument(
                "--pause-ms",
                type=float,
                default=0,
                help="controlled SIGSTOP of sender after capture starts",
            )
            p.add_argument("--pause-at-ms", type=float, default=100)
    args = parser.parse_args()
    if args.duration_ms < 120 or any(
        not math.isfinite(w) or round(w * 1e6) < 1 or w > args.duration_ms
        for w in args.windows_ms
    ):
        parser.error(
            "duration must be >=120ms; windows must be positive and <=duration"
        )
    if not 0 < args.tolerance < 1:
        parser.error("tolerance must be between zero and one")
    rates = args.rates if args.command == "run" else [args.rate]
    if any(not math.isfinite(r) or r * 1e6 / 8 / 1400 < 1 for r in rates):
        parser.error("rates must be finite and at least one packet/s")
    if args.command == "run":
        ipaddress.IPv4Address(args.ip)
        if len({f"{rate:g}" for rate in args.rates}) != len(args.rates):
            parser.error(
                "rates must have unique names at six significant digits (prevents overwriting captures)"
            )
        if (
            args.repeats < 1
            or (args.compete and args.sender_cpu is None)
            or args.pause_ms < 0
            or args.pause_at_ms < 0
        ):
            parser.error(
                "positive repeats required; --compete requires --sender-cpu; pauses must be nonnegative"
            )
        return run(args)
    report, valid, _ = analyze(
        args.pcap,
        args.csv,
        args.rate,
        args.duration_ms,
        args.windows_ms,
        args.tolerance,
        args.capture_log,
        args.sender_log,
    )
    print(report)
    return 0 if valid else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (
        OSError,
        ValueError,
        RuntimeError,
        TimeoutError,
        subprocess.TimeoutExpired,
    ) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
