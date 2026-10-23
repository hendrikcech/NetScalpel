#!/usr/bin/env python3
"""Verify NetScalpel end-to-end measurement output against netem ground truth.

Parses the UDP CSV written by `scalpel-run` (seq, ts_sent, ts_rcvd, owd_ms,
size, lost, dup_count, and optional ts_rcvd_1..N columns) and asserts that
the measured values match the emulated link. Exits
non-zero on any violation; the nixnet experiment uses this exit code as the
test verdict.

Requires Python >= 3.11 (datetime.fromisoformat with nanosecond timestamps).
"""

import argparse
import csv
import math
import os
import statistics
import sys
from pathlib import Path

# Support direct invocation from either the checkout or a copied Nix store root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from measurements.udp import Row, parse_rows, check_common, owd_summary, quantile, parse_timestamp

MAX_PRINTED_ERRORS = 20

# Logged by txtsreader.go when kernel TX timestamping could not be enabled or
# returned an unexpected number of timestamps (userspace fallback).
TXTS_FALLBACK_MARKERS = [
    "Failed enabling tx timestamping",
    "using timestamps from run function",
    "TxTsReader was not used, use sentMsgs from Run func",
]
TXTS_SUCCESS_MARKER = "selectMsgsSent: returning with txTsReader infos"


def owd_stats(rows: list[Row]) -> str:
    owds = [r.owd_ms for r in rows if not r.lost]
    if not owds:
        return "no received packets"
    return (
        f"n={len(rows)} lost={sum(r.lost for r in rows)} "
        f"owd_ms min={min(owds):.3f} median={statistics.median(owds):.3f} "
        f"max={max(owds):.3f}"
    )


def cmd_owd(args) -> list[str]:
    errors: list[str] = []
    rows = parse_rows(args.csv, errors)
    check_common(args.csv, rows, args.min_packets, errors)

    lost = sum(r.lost for r in rows)
    if lost > args.max_lost:
        errors.append(f"{args.csv}: {lost} packets lost, expected <= {args.max_lost}")

    lo = args.delay_ms - args.eps_below_ms
    hi = args.delay_ms + args.eps_ms
    received = [r for r in rows if not r.lost]
    profile = getattr(args, "profile", "smoke")
    outliers = [r for r in received if r.owd_ms > hi]
    if not received:
        errors.append(f"{args.csv}: no received packets")
    elif profile == "smoke":
        median = owd_summary(rows)["owd_ms"]["median"]
        if not lo <= median <= hi:
            errors.append(f"{args.csv}: median owd {median:.3f} ms outside [{lo}, {hi}]")
        fraction = len(outliers) / len(received)
        if fraction > args.max_outlier_fraction:
            errors.append(f"{args.csv}: outlier fraction {fraction:.6f} > {args.max_outlier_fraction}")
    for r in received:
        cap = hi if profile == "strict" else args.delay_ms + args.max_excess_ms
        if not lo <= r.owd_ms <= cap:
            errors.append(
                f"{args.csv}: seq {r.seq} owd {r.owd_ms:.3f} ms outside [{lo}, {cap}]"
            )

    print(f"owd {args.csv}: {owd_stats(rows)} (profile {profile}, bounds [{lo}, {hi}], "
          f"outliers={len(outliers)}/{len(received)}, outlier_seqs={[r.seq for r in outliers]})")
    return errors


def cmd_loss(args) -> list[str]:
    errors: list[str] = []
    rows = parse_rows(args.csv, errors)
    check_common(args.csv, rows, args.min_packets, errors)
    if not rows:
        return errors

    n = len(rows)
    lost = sum(r.lost for r in rows)
    p = args.loss_percent / 100
    sigma = math.sqrt(p * (1 - p) / n)
    lo, hi = p - args.z * sigma, p + args.z * sigma
    if args.profile == "smoke":
        # A gross response check, not a statistical confidence claim.
        lo, hi = max(0, p - args.smoke_loss_tolerance), min(1, p + args.smoke_loss_tolerance)
    observed = lost / n
    if not lo <= observed <= hi:
        errors.append(
            f"{args.csv}: loss rate {observed:.4f} ({lost}/{n}) outside "
            f"[{lo:.4f}, {hi:.4f}] (profile={args.profile}, p={p})"
        )

    print(
        f"loss {args.csv}: {lost}/{n} lost ({observed * 100:.2f}%), "
        f"profile={args.profile}, allowed [{lo * 100:.2f}%, {hi * 100:.2f}%]"
    )
    return errors


def cmd_rate(args) -> list[str]:
    """Assert a rate-limited bottleneck: goodput pinned at the netem rate and
    owd_ms growing from the bare delay to a queue-full plateau bounded by the
    netem limit. All owd bounds are derived from the configured rate/limit."""
    errors: list[str] = []
    rows = parse_rows(args.csv, errors)
    check_common(args.csv, rows, args.min_packets, errors)
    rcvd = [r for r in rows if not r.lost]
    if len(rcvd) < 2:
        errors.append(f"{args.csv}: fewer than 2 received packets")
        return errors

    # netem's rate emulation operates on the wire size (payload + UDP/IP/
    # ethernet headers), the CSV records payload bytes.
    wire_bits = sum((r.size + args.overhead_bytes) * 8 for r in rcvd)
    span_s = (rcvd[-1].ts_rcvd_ns - rcvd[0].ts_rcvd_ns) / 1e9
    if span_s <= 0:
        errors.append(f"{args.csv}: receive span must be positive")
        return errors
    rate_mbit = wire_bits / span_s / 1e6
    if args.profile == "smoke":
        if len(rcvd) < max(2, math.ceil(len(rows) * .1)):
            errors.append(f"{args.csv}: fewer than 10% of sent packets received")
        # Scheduling and short-run startup dominate performance in small VMs.
        # Still require a physically plausible positive rate and delayed packets.
        if not 0 < rate_mbit <= 1.5 * args.rate_mbit:
            errors.append(f"{args.csv}: receive rate {rate_mbit:.2f} Mbps outside (0, {1.5 * args.rate_mbit}]")
        owds = [r.owd_ms for r in rcvd]
        floor = max(0, args.delay_ms - 2)
        if min(owds) < floor:
            errors.append(f"{args.csv}: min owd {min(owds):.3f} ms below delay floor {floor}")
        ser_ms = (rcvd[0].size + args.overhead_bytes) * 8 / (args.rate_mbit * 1e6) * 1e3
        median_cap = args.delay_ms + 4 * args.limit * ser_ms + 100
        if statistics.median(owds) > median_cap:
            errors.append(f"{args.csv}: median owd exceeds broad queue bound {median_cap:.3f} ms")
        print(f"rate {args.csv}: smoke {rate_mbit:.2f} Mbps (target {args.rate_mbit}), {owd_stats(rows)}")
        return errors
    lo, hi = args.rate_mbit * (1 - args.tolerance), args.rate_mbit * (1 + args.tolerance)
    if not lo <= rate_mbit <= hi:
        errors.append(
            f"{args.csv}: receive rate {rate_mbit:.2f} Mbps outside [{lo:.2f}, {hi:.2f}]"
        )

    # Sojourn of a packet through a full queue of `limit` packets.
    ser_ms = (rcvd[0].size + args.overhead_bytes) * 8 / (args.rate_mbit * 1e6) * 1e3
    max_queue_ms = args.limit * ser_ms

    owds = [r.owd_ms for r in rcvd]
    first = owds[0]
    if not args.delay_ms - 1 <= first <= args.delay_ms + 5:
        errors.append(
            f"{args.csv}: first packet owd {first:.3f} ms not close to the bare "
            f"delay {args.delay_ms} ms (queue should start empty)"
        )
    if min(owds) < args.delay_ms - 1:
        errors.append(f"{args.csv}: min owd {min(owds):.3f} ms below delay {args.delay_ms} ms")
    p90 = quantile(owds, 0.9)
    grown = args.delay_ms + 0.5 * max_queue_ms
    if p90 < grown:
        errors.append(
            f"{args.csv}: p90 owd {p90:.3f} ms < {grown:.3f} ms; queue did not fill"
        )
    cap = args.delay_ms + 1.3 * max_queue_ms + 5
    if max(owds) > cap:
        errors.append(
            f"{args.csv}: max owd {max(owds):.3f} ms > {cap:.3f} ms; queue exceeded "
            f"the configured limit of {args.limit} packets"
        )

    print(
        f"rate {args.csv}: {rate_mbit:.2f} Mbps (target {args.rate_mbit}), "
        f"{owd_stats(rows)}, p90={p90:.3f}, queue-full plateau "
        f"~{args.delay_ms + max_queue_ms:.1f} ms"
    )
    return errors


def cmd_compare(args) -> list[str]:
    """Assert that the median OWDs of two measurements agree within eps."""
    errors: list[str] = []
    medians = []
    for path in (args.csv_a, args.csv_b):
        rows = parse_rows(path, errors)
        check_common(path, rows, 1, errors)
        owds = [r.owd_ms for r in rows if not r.lost]
        if not owds:
            errors.append(f"{path}: no received packets")
            return errors
        medians.append(statistics.median(owds))
    diff = abs(medians[0] - medians[1])
    if diff > args.eps_ms:
        errors.append(
            f"medians disagree by {diff:.3f} ms > {args.eps_ms} ms: "
            f"{args.csv_a}={medians[0]:.3f}, {args.csv_b}={medians[1]:.3f}"
        )
    print(
        f"compare: median owd {args.csv_a}={medians[0]:.3f} ms, "
        f"{args.csv_b}={medians[1]:.3f} ms, diff={diff:.3f} ms (eps {args.eps_ms})"
    )
    return errors


def cmd_tcp(args) -> list[str]:
    """Sanity-check the TCP sender-metrics CSV against a delay+rate link."""
    errors: list[str] = []
    try:
        with open(args.csv, newline="") as f:
            rows = list(csv.DictReader(f))
    except (OSError, csv.Error, UnicodeError) as exc:
        return [f"{args.csv}: cannot read CSV: {exc}"]
    if len(rows) < args.min_rows:
        errors.append(f"{args.csv}: only {len(rows)} rows, expected >= {args.min_rows}")
        return errors

    try:
        times = []
        acked = []
        for row in rows:
            if None in row or any(value is None for value in row.values()):
                raise ValueError("row width does not match header")
            times.append(parse_timestamp(row["Time"])[1])
            # Validate every numeric sample, not just the ones used for gates.
            for key, value in row.items():
                if key in ("Time", "State", "CAState"):
                    continue
                if key in ("BBRMaxBW", "BBRMinRTT", "BBRPacingGain", "BBRCongWindowGain") and value == "":
                    continue
                number = float(value)
                if not math.isfinite(number) or number < 0:
                    raise ValueError(f"{key} must be finite and nonnegative")
            acked.append(int(row["ThruBytesAcked"]))
        if any(a > b for a, b in zip(times, times[1:])):
            raise ValueError("Time moves backwards")
        if any(a > b for a, b in zip(acked, acked[1:])):
            raise ValueError("ThruBytesAcked moves backwards")
        span_s = (times[-1] - times[0]) / 1e9
        if span_s <= 0:
            raise ValueError("measurement span must be positive")
        first_rtt = float(rows[0]["RTT"])
        rtts = [float(r["RTT"]) for r in rows]
        min_rtts = [float(r["MinRTT"]) for r in rows if float(r["MinRTT"]) > 0]
    except (KeyError, ValueError, TypeError, OverflowError) as exc:
        return errors + [f"{args.csv}: invalid TCP data: {exc}"]
    if not min_rtts:
        return errors + [f"{args.csv}: no MinRTT samples"]
    if args.profile == "smoke":
        floor, cap = args.rtt_ms * .5, args.rtt_ms * 4 + 100
        for label, value in (("median RTT", statistics.median(rtts)), ("MinRTT", min(min_rtts))):
            if not floor <= value <= cap:
                errors.append(f"{args.csv}: {label} {value:.3f} ms outside broad [{floor}, {cap}]")
        acked_mbit = (acked[-1] - acked[0]) * 8 / span_s / 1e6
        if not 0 < acked_mbit <= args.rate_mbit * 2:
            errors.append(f"{args.csv}: throughput {acked_mbit:.2f} Mbps outside (0, {args.rate_mbit * 2}]")
        print(f"tcp {args.csv}: smoke n={len(rows)}, median RTT={statistics.median(rtts):.1f} ms, "
              f"MinRTT={min(min_rtts):.1f} ms, throughput={acked_mbit:.2f} Mbps")
        return errors
    if not args.rtt_ms - 2 <= first_rtt <= args.rtt_ms + 20:
        errors.append(
            f"{args.csv}: RTT at start {first_rtt:.1f} ms not within "
            f"[{args.rtt_ms - 2}, {args.rtt_ms + 20}] (path RTT {args.rtt_ms} ms)"
        )

    # MinRTT is the kernel's minimum filter; 0 means not (yet) available.
    min_rtt = min(min_rtts)
    if not args.rtt_ms - 2 <= min_rtt <= args.rtt_ms + 6:
        errors.append(
            f"{args.csv}: MinRTT {min_rtt:.1f} ms not within "
            f"[{args.rtt_ms - 2}, {args.rtt_ms + 6}]"
        )

    acked_mbit = (acked[-1] - acked[0]) * 8 / span_s / 1e6
    if acked_mbit > args.rate_mbit * 1.02:
        errors.append(
            f"{args.csv}: throughput {acked_mbit:.2f} Mbps exceeds link rate {args.rate_mbit} Mbps"
        )
    if acked_mbit < args.rate_mbit * 0.5:
        errors.append(
            f"{args.csv}: throughput {acked_mbit:.2f} Mbps < half the link rate; "
            f"sender never approached the bottleneck"
        )

    print(
        f"tcp {args.csv}: n={len(rows)} first RTT={first_rtt:.1f} ms, "
        f"MinRTT={min_rtt:.1f} ms, acked throughput={acked_mbit:.2f} Mbps "
        f"(link {args.rate_mbit} Mbps)"
    )
    return errors


def cmd_txts(args) -> list[str]:
    """Assert that kernel TX timestamping was used (no userspace fallback).

    The fallback still produces valid CSVs, but it is not the code path that
    production measurements rely on, so the e2e run must catch it.
    """
    errors: list[str] = []
    for path in args.log:
        try:
            with open(path) as log:
                content = log.read()
        except OSError as e:
            errors.append(f"{path}: cannot read log: {e}")
            continue
        for marker in TXTS_FALLBACK_MARKERS:
            if marker in content:
                errors.append(f"{path}: TX timestamp fallback detected: {marker!r}")
        if TXTS_SUCCESS_MARKER not in content:
            errors.append(f"{path}: missing positive TX timestamp marker {TXTS_SUCCESS_MARKER!r}")
    print(f"txts: checked {len(args.log)} log(s) for kernel TX timestamp evidence")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    def profile(p):
        p.add_argument("--profile", choices=("smoke", "strict"),
                       default=os.environ.get("NETSCALPEL_TEST_PROFILE", "smoke"),
                       help="smoke correctness checks or strict performance bounds (default: NETSCALPEL_TEST_PROFILE or smoke)")

    p = sub.add_parser("owd", help="assert one-way delay matches netem delay")
    p.add_argument("--csv", required=True)
    p.add_argument("--delay-ms", type=float, required=True, help="netem delay (ground truth)")
    p.add_argument("--eps-ms", type=float, default=2.0, help="allowed excess over the netem delay")
    p.add_argument("--eps-below-ms", type=float, default=1.0, help="allowed shortfall below the netem delay")
    p.add_argument("--max-lost", type=int, default=0)
    p.add_argument("--min-packets", type=int, default=1)
    profile(p)
    p.add_argument("--max-outlier-fraction", type=float, default=0.01,
                   help="smoke: maximum fraction above delay + eps (all packets retained)")
    p.add_argument("--max-excess-ms", type=float, default=50.0,
                   help="smoke: absolute per-packet excess cap above delay")
    p.set_defaults(func=cmd_owd)

    p = sub.add_parser("loss", help="assert loss rate is within a binomial interval")
    p.add_argument("--csv", required=True)
    p.add_argument("--loss-percent", type=float, required=True, help="netem loss (ground truth)")
    p.add_argument("--z", type=float, default=4.0, help="width of the acceptance interval in standard deviations")
    p.add_argument("--min-packets", type=int, default=2000)
    profile(p)
    p.add_argument("--smoke-loss-tolerance", type=float, default=.03,
                   help="smoke: absolute loss-fraction tolerance around configured loss")
    p.set_defaults(func=cmd_loss)

    p = sub.add_parser("rate", help="assert bottleneck rate and queue-buildup owd")
    p.add_argument("--csv", required=True)
    p.add_argument("--delay-ms", type=float, required=True, help="netem delay (ground truth)")
    p.add_argument("--rate-mbit", type=float, required=True, help="netem rate (ground truth)")
    p.add_argument("--limit", type=int, required=True, help="netem queue limit in packets")
    p.add_argument("--overhead-bytes", type=int, default=42, help="ethernet+IP+UDP header bytes counted by netem's rate emulation")
    p.add_argument("--tolerance", type=float, default=0.1, help="relative tolerance for the receive rate")
    p.add_argument("--min-packets", type=int, default=1000)
    profile(p)
    p.set_defaults(func=cmd_rate)

    p = sub.add_parser("compare", help="assert two measurements agree on the median owd")
    p.add_argument("--csv-a", required=True)
    p.add_argument("--csv-b", required=True)
    p.add_argument("--eps-ms", type=float, default=1.0)
    p.set_defaults(func=cmd_compare)

    p = sub.add_parser("tcp", help="sanity-check TCP metrics against a delay+rate link")
    p.add_argument("--csv", required=True)
    p.add_argument("--rtt-ms", type=float, required=True, help="path round-trip time (2x netem delay)")
    p.add_argument("--rate-mbit", type=float, required=True, help="netem rate (ground truth)")
    p.add_argument("--min-rows", type=int, default=50)
    profile(p)
    p.set_defaults(func=cmd_tcp)

    p = sub.add_parser("txts", help="assert kernel TX timestamps were used")
    p.add_argument("--log", action="append", required=True, help="scalpel-run debug log (repeatable)")
    p.set_defaults(func=cmd_txts)

    args = parser.parse_args()
    if hasattr(args, "profile") and args.profile not in ("smoke", "strict"):
        parser.error("NETSCALPEL_TEST_PROFILE must be smoke or strict")
    # Reject nonsensical policy values before reading any measurement data.
    for name, value in vars(args).items():
        if isinstance(value, (int, float)) and (not math.isfinite(value) or value < 0):
            parser.error(f"--{name.replace('_', '-')} must be finite and nonnegative")
    for name in ("min_packets", "min_rows", "rate_mbit", "limit", "z"):
        if hasattr(args, name) and getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    for name, maximum in (("max_outlier_fraction", 1), ("tolerance", 1), ("loss_percent", 100),
                          ("smoke_loss_tolerance", 1)):
        if hasattr(args, name) and getattr(args, name) > maximum:
            parser.error(f"--{name.replace('_', '-')} must be <= {maximum}")
    if args.cmd == "owd" and args.max_excess_ms < args.eps_ms:
        parser.error("--max-excess-ms must be >= --eps-ms")
    errors = args.func(args)

    if errors:
        for e in errors[:MAX_PRINTED_ERRORS]:
            print(f"FAIL: {e}", file=sys.stderr)
        if len(errors) > MAX_PRINTED_ERRORS:
            print(f"... and {len(errors) - MAX_PRINTED_ERRORS} more", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
