#!/usr/bin/env python3
"""Strict, standard-library-only analysis of RateSender benchmark artifacts."""
import argparse
import csv
from datetime import datetime
import json
import math
from pathlib import Path
import statistics
import sys

CASES = (
    "single500", "single100", "equal500", "equal100", "up100to500",
    "down500to100", "d2_capped", "d2_uncapped", "single7pps",
    "split7pps", "short7pps",
)

ENVIRONMENT_KEYS = (
    "go_version", "kernel_version", "gomaxprocs", "cpus", "goos", "goarch",
    "go_environment", "payload_bytes", "timestamp_source", "suite_version", "benchmark_sha256",
)


def check_float(actual, expected, name):
    # Timestamps are compared as integer ns; this tolerance is only for derived
    # floating point units and Go/Python summation-order differences.
    if (isinstance(actual, bool) or not isinstance(actual, (int, float)) or
            not math.isfinite(actual) or
            not math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-9)):
        raise ValueError(f"{name}: {actual!r} != recomputed {expected!r}")


def environment_differences(before, after):
    return {key: {"baseline": before[key], "candidate": after[key]}
            for key in ENVIRONMENT_KEYS if before[key] != after[key]}


def distribution(values):
    """Median/range across repetitions, not a pooled packet population."""
    values = list(values)
    if not values:
        return {"median": None, "min": None, "max": None, "n": 0}
    if any(not math.isfinite(v) for v in values):
        raise ValueError("nonfinite measurement")
    return {"median": statistics.median(values), "min": min(values),
            "max": max(values), "n": len(values)}


def percentile(values, fraction):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def schedule(case):
    rate = lambda mbps, ms: (mbps * 1_000_000 // 8 // 1408, ms)
    if case in ("single500", "single100"):
        return [rate(int(case[6:]), 1200)]
    if case in ("equal500", "equal100"):
        return [rate(int(case[5:]), 600)] * 2
    if case == "up100to500":
        return [rate(100, 600), rate(500, 600)]
    if case == "down500to100":
        return [rate(500, 600), rate(100, 600)]
    if case in ("d2_capped", "d2_uncapped"):
        return [rate(500, 8), rate(0, 12)] * 50 + [rate(500, 600)]
    if case == "single7pps":
        return [(7, 1200)]
    if case == "split7pps":
        return [(7, 600)] * 2
    if case == "short7pps":
        return [(7, 100)] * 12
    raise ValueError(f"unknown case: {case}")


def validate(record, directory):
    case = record.get("case")
    if case not in CASES:
        raise ValueError(f"unknown case: {case}")
    if record.get("schema") != 1 or record.get("kernel") is not True:
        raise ValueError("requires schema 1 and complete kernel TX timestamps")
    meta = record["metadata"]
    for key in ("created_utc", "go_version", "goos", "goarch", "gomaxprocs",
                "git_revision", "git_status", "source_path", "source_sha256", "benchmark_sha256",
                "kernel_version", "argv", "suite_version", "legacy_prefix", "capped",
                "cpus", "go_environment", "start_unix_ns"):
        if key not in meta:
            raise ValueError(f"missing provenance: {key}")
    if not isinstance(meta["created_utc"], str):
        raise ValueError("invalid created_utc")
    created = datetime.fromisoformat(meta["created_utc"].replace("Z", "+00:00"))
    if created.utcoffset() is None:
        raise ValueError("created_utc must include timezone")
    for key in ("start_unix_ns", "cpus", "gomaxprocs"):
        if type(meta[key]) is not int or meta[key] <= 0:
            raise ValueError(f"invalid {key}")
    if (not isinstance(meta["go_environment"], dict) or
            set(meta["go_environment"]) != {"GOFLAGS", "GOGC", "GOMEMLIMIT"} or
            not all(isinstance(v, str) for v in meta["go_environment"].values())):
        raise ValueError("invalid Go tuning environment")
    if (meta.get("payload_bytes") != 1408 or meta["suite_version"] != 1 or
            meta.get("timestamp_source") != "complete kernel software TX"):
        raise ValueError("incompatible accounting/timestamp source")
    if meta["capped"] != (case != "d2_uncapped"):
        raise ValueError("unexpected outer deadline mode")
    if type(record["packets"]) is not int or record["packets"] < 0:
        raise ValueError("invalid total packet count")
    for key in ("source_sha256", "benchmark_sha256"):
        if (not isinstance(meta[key], str) or len(meta[key]) != 64 or
                any(c not in "0123456789abcdef" for c in meta[key])):
            raise ValueError(f"invalid {key}")
    phases = record["phases"]
    expected = schedule(case)
    if len(phases) != len(expected):
        raise ValueError("incomplete schedule metadata")
    nominal = 0
    observed = []
    numeric_fields = ("start_ms", "drift_ms", "first_ms", "last_ms", "span_ms",
                      "mbps", "achieved_pps", "budget_ms")
    for i, (p, (pps, duration)) in enumerate(zip(phases, expected)):
        if (p["segment"], p["pps"], p["duration_ms"], p["nominal_ms"], p["expected_packets"]) != (
                i, pps, duration, nominal, pps * duration // 1000):
            raise ValueError(f"changed schedule/accounting in segment {i}")
        if type(p["observed"]) is not bool or type(p["packets"]) is not int or p["packets"] < 0:
            raise ValueError("invalid phase observation/count")
        for key in numeric_fields:
            value = p[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"nonfinite/invalid phase {key}")
        if p["observed"]:
            if p["start_ms"] < 0:
                raise ValueError("negative observed start")
            check_float(p["drift_ms"], p["start_ms"] - nominal, "phase drift_ms")
            start_ns = round(p["start_ms"] * 1_000_000)
            check_float(p["start_ms"], start_ns / 1_000_000, "phase start ns precision")
            if observed and start_ns < observed[-1][1]:
                raise ValueError("nonmonotonic observed starts")
            observed.append((i, start_ns))
        else:
            for key in numeric_fields:
                check_float(p[key], 0, f"unobserved {key}")
        if not p["observed"] and p["packets"]:
            raise ValueError("packets in unobserved segment")
        if pps == 0 and p["packets"]:
            raise ValueError("packets in cooldown")
        nominal += duration
    if not observed:
        raise ValueError("no observed segment starts")
    if sum(p["packets"] for p in phases) != record["packets"]:
        raise ValueError("phase packet count mismatch")
    csv_name = record["csv"]
    if not csv_name or Path(csv_name).name != csv_name:
        raise ValueError("CSV must be a local basename")
    counts = [0] * len(phases)
    previous = None
    previous_segment = -1
    timestamps = []
    per_phase = [[] for _ in phases]
    count = 0
    with (directory / csv_name).open(newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != ["seq", "kernel_tx_unix_ns", "elapsed_ns", "segment", "udp_bytes"]:
            raise ValueError("invalid raw CSV columns")
        for count, row in enumerate(reader, 1):
            ts, segment = int(row["kernel_tx_unix_ns"]), int(row["segment"])
            if (int(row["seq"]) != count - 1 or ts <= 0 or
                    (previous is not None and ts < previous) or
                    int(row["udp_bytes"]) != 1408 or not 0 <= segment < len(phases) or
                    segment < previous_segment):
                raise ValueError("invalid/incomplete kernel CSV")
            if int(row["elapsed_ns"]) != ts - meta["start_unix_ns"]:
                raise ValueError("elapsed_ns differs from kernel TX minus Run start")
            counts[segment] += 1
            per_phase[segment].append((count - 1, ts))
            timestamps.append(ts)
            previous = ts
            previous_segment = segment
    if count != record["packets"] or counts != [p["packets"] for p in phases]:
        raise ValueError("raw CSV packet count mismatch")
    limit_ns = nominal * 1_000_000 + (0 if meta["capped"] else 1_000_000_000)
    for j, (i, start_ns) in enumerate(observed):
        p = phases[i]
        end_ns = observed[j + 1][1] if j + 1 < len(observed) else limit_ns
        budget_ns = max(0, min(round(p["duration_ms"] * 1_000_000), end_ns - start_ns))
        check_float(p["budget_ms"], budget_ns / 1_000_000, "phase budget_ms")
    for i, p in enumerate(phases):
        packets = per_phase[i]
        if packets:
            index, first = packets[0]
            last = packets[-1][1]
            check_float(p["first_ms"], (first - meta["start_unix_ns"]) / 1_000_000, "phase first_ms")
            check_float(p["last_ms"], (last - meta["start_unix_ns"]) / 1_000_000, "phase last_ms")
            check_float(p["span_ms"], (last - first) / 1_000_000, "phase span_ms")
            if index:
                check_float(p["boundary_gap_us"], (first - timestamps[index - 1]) / 1000, "boundary_gap_us")
            elif p["boundary_gap_us"] is not None:
                raise ValueError("first packet phase must have null boundary gap")
            budget_ns = round(p["budget_ms"] * 1_000_000)
            if budget_ns <= 0:
                raise ValueError("packets without positive budget")
            pps = len(packets) * 1_000_000_000 / budget_ns
            check_float(p["achieved_pps"], pps, "phase achieved_pps")
            check_float(p["mbps"], pps * 1408 * 8 / 1_000_000, "phase mbps")
        else:
            for key in ("first_ms", "last_ms", "span_ms", "achieved_pps", "mbps"):
                check_float(p[key], 0, f"empty phase {key}")
            if p["boundary_gap_us"] is not None:
                raise ValueError("empty phase must have null boundary gap")
    gaps = [(b - a) / 1000 for a, b in zip(timestamps, timestamps[1:])]
    for key, value in (("gap_median_us", statistics.median(gaps) if gaps else 0),
                       ("gap_p99_us", percentile(gaps, .99) if gaps else 0),
                       ("gap_stddev_us", statistics.pstdev(gaps) if gaps else 0)):
        check_float(record[key], value, key)
    baseline = []
    if case in ("single500", "single100"):
        # Exact same crossing definition as the Go suite, using raw integer ns.
        index = 1
        for ms in range(100, 1101, 20):
            at = meta["start_unix_ns"] + ms * 1_000_000
            while index < len(timestamps) and timestamps[index] < at:
                index += 1
            if index < len(timestamps) and timestamps[index - 1] < at:
                baseline.append((timestamps[index] - timestamps[index - 1]) / 1000)
    saved_baseline = record["baseline_gaps_us"] or []
    if len(saved_baseline) != len(baseline):
        raise ValueError("baseline virtual crossing gap count mismatch")
    for saved, raw in zip(saved_baseline, baseline):
        check_float(saved, raw, "baseline virtual crossing gap")


def run_metrics(record):
    active = [p for p in record["phases"] if p["pps"]]
    final = active[-1]
    metrics = {"packets": record["packets"],
               "gap_median_us": record["gap_median_us"],
               "gap_p99_us": record["gap_p99_us"],
               "gap_stddev_us": record["gap_stddev_us"],
               "unobserved_active_segments": sum(not p["observed"] for p in active),
               "final_observed": int(final["observed"])}
    # Missing phases are NEVER silently turned into zero drift/rate/span.
    if final["observed"]:
        for name in ("drift_ms", "span_ms", "packets", "mbps", "achieved_pps", "budget_ms"):
            metrics["final_" + name] = final[name]
        if final["boundary_gap_us"] is not None:
            metrics["final_boundary_gap_us"] = final["boundary_gap_us"]
    gaps = [p["boundary_gap_us"] for p in active if p["boundary_gap_us"] is not None]
    if gaps:
        metrics["boundary_gap_median_us"] = statistics.median(gaps)
        metrics["boundary_gap_max_us"] = max(gaps)
    baseline = record["baseline_gaps_us"] or []
    if baseline:
        metrics["baseline_gap_median_us"] = statistics.median(baseline)
        metrics["baseline_gap_p99_us"] = percentile(baseline, .99)
        metrics["baseline_gap_max_us"] = max(baseline)
        metrics["baseline_gap_stddev_us"] = statistics.pstdev(baseline)
    if record["case"].startswith("d2_"):
        pulses = active[:-1]
        metrics.update({
            "pulse_packets": sum(p["packets"] for p in pulses),
            "pulse_expected_packets": sum(p["expected_packets"] for p in pulses),
            "pulse_min_packets": min(p["packets"] for p in pulses),
            "pulse_max_packets": max(p["packets"] for p in pulses),
            "pulse_observed": sum(p["observed"] for p in pulses),
            "pulse_budget_ratio": sum(p["packets"] for p in pulses) / sum(p["expected_packets"] for p in pulses),
        })
        drifts = [p["drift_ms"] for p in pulses if p["observed"]]
        if drifts:
            metrics["pulse_max_drift_ms"] = max(drifts)
    return metrics


def summarize(records):
    groups = {}
    for record in records:
        case = record["case"]
        if case not in CASES:
            raise ValueError(f"unknown case: {case}")
        groups.setdefault(case, []).append(run_metrics(record))
    result = {}
    for case, runs in groups.items():
        keys = sorted(set().union(*(r.keys() for r in runs)))
        result[case] = {key: distribution(r[key] for r in runs if key in r) for key in keys}
    return result


def load(directory, cases, repetitions):
    directory = Path(directory)
    records = []
    csv_refs, run_ids, run_starts = set(), set(), set()
    for path in sorted(directory.glob("*.csv.json")):
        try:
            record = json.loads(path.read_text())
            csv_name = record["csv"]
            csv_ref = (directory / csv_name).resolve()
            if csv_ref in csv_refs:
                raise ValueError("duplicate CSV reference")
            if path.name != csv_name + ".json":
                raise ValueError("sidecar name does not match CSV basename")
            validate(record, directory)
            meta = record["metadata"]
            run_id = (meta["created_utc"], meta["start_unix_ns"])
            if run_id in run_ids or meta["start_unix_ns"] in run_starts:
                raise ValueError("duplicate run identity/Run start")
            csv_refs.add(csv_ref)
            run_ids.add(run_id)
            run_starts.add(meta["start_unix_ns"])
        except (KeyError, TypeError, ValueError, OSError) as e:
            raise ValueError(f"{path}: {e}") from e
        records.append(record)
    if not records:
        raise ValueError(f"{directory}: no benchmark JSON artifacts")
    raw_csvs = {path.resolve() for path in directory.glob("*.csv")}
    if raw_csvs != csv_refs:
        raise ValueError(f"{directory}: raw CSV set differs from validated sidecar references")
    actual = {r["case"] for r in records}
    if actual != set(cases):
        raise ValueError(f"{directory}: cases differ: expected {sorted(cases)}, found {sorted(actual)}")
    for case in cases:
        n = sum(r["case"] == case for r in records)
        if n != repetitions:
            raise ValueError(f"{directory}: {case} has {n} runs, expected {repetitions}")
    # Mixing source versions inside one directory destroys the comparison.
    provenance = {json.dumps({k: r["metadata"][k] for k in (
        "source_sha256", "git_revision", "legacy_prefix", *ENVIRONMENT_KEYS)}, sort_keys=True)
        for r in records}
    if len(provenance) != 1:
        raise ValueError(f"{directory}: mixed build/runtime provenance")
    return records


def compare(before, after):
    if set(before) != set(after):
        raise ValueError("comparison case sets differ")
    return {case: {key: after[case][key]["median"] - before[case][key]["median"]
                   for key in before[case].keys() & after[case].keys()
                   if before[case][key]["median"] is not None and after[case][key]["median"] is not None}
            for case in before}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path, nargs="?")
    parser.add_argument("--repetitions", type=int, default=6)
    parser.add_argument("--cases", default=",".join(CASES), help="explicit comma-separated subset")
    parser.add_argument("--json", type=Path, help="write complete machine-readable comparison")
    parser.add_argument("--allow-environment-differences", action="store_true",
                        help="allow intentional before/after runtime/accounting differences (recorded and warned)")
    args = parser.parse_args(argv)
    cases = args.cases.split(",")
    if args.repetitions < 1 or len(set(cases)) != len(cases) or not set(cases) <= set(CASES):
        parser.error("invalid repetitions or unknown/duplicate cases")
    try:
        baseline = load(args.baseline, cases, args.repetitions)
        report = {"schema": 1, "baseline": summarize(baseline),
                  "baseline_provenance": baseline[0]["metadata"]}
        if args.candidate:
            candidate = load(args.candidate, cases, args.repetitions)
            differences = environment_differences(baseline[0]["metadata"], candidate[0]["metadata"])
            if differences and not args.allow_environment_differences:
                raise ValueError("before/after environment/accounting differs: " +
                                 ", ".join(differences) + "; intentional changes require --allow-environment-differences")
            report["environment_differences"] = differences
            report["environment_differences_allowed"] = args.allow_environment_differences
            if differences:
                print("WARNING: allowing before/after environment/accounting differences: " +
                      json.dumps(differences, sort_keys=True), file=sys.stderr)
            report["candidate"] = summarize(candidate)
            report["candidate_provenance"] = candidate[0]["metadata"]
            report["median_delta_candidate_minus_baseline"] = compare(report["baseline"], report["candidate"])
        if args.json:
            args.json.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
        for label in ("baseline", "candidate"):
            if label not in report:
                continue
            print(label)
            for case, metrics in report[label].items():
                print(f"  {case}")
                for key, d in metrics.items():
                    print(f"    {key}: {d['median']:.6g} [{d['min']:.6g}, {d['max']:.6g}] n={d['n']}")
        if "candidate" in report:
            print("Median deltas (candidate - baseline)")
            for case, metrics in report["median_delta_candidate_minus_baseline"].items():
                print(f"  {case}: " + ", ".join(f"{k}={v:+.6g}" for k, v in sorted(metrics.items())))
    except (ValueError, OSError) as e:
        parser.exit(1, f"error: {e}\n")


if __name__ == "__main__":
    main()
