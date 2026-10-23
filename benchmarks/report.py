#!/usr/bin/env python3
"""Offline, standard-library-only pacing and UDP artifact reports."""
import argparse
import csv
import hashlib
from html import escape
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmarks import analyze, throughput


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def cases_argument(value):
    cases = value.split(",")
    if not cases or len(cases) != len(set(cases)) or not set(cases) <= set(analyze.CASES):
        raise argparse.ArgumentTypeError("unknown, empty, or duplicate pacing cases")
    return cases


def inventory(paths):
    result = []
    for path in paths:
        if path.is_file():
            try:
                result.append({"path": str(path.resolve()), "bytes": path.stat().st_size,
                               "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
            except OSError as error:
                result.append({"path": str(path.resolve()), "error": str(error)})
    return result


def flatten(value, prefix=""):
    result = {}
    for key, item in value.items():
        name = prefix + key
        if isinstance(item, dict):
            result.update(flatten(item, name + "."))
        elif item is None or (type(item) in (int, float) and math.isfinite(item)):
            result[name] = item
    return result


def aggregate(runs):
    groups = {}
    for run in runs:
        if run["validity"]["status"] == "valid":
            groups.setdefault(run["case"], []).append(flatten(run["metrics"]))
    return {case: {key: analyze.distribution(r[key] for r in values
                                           if r.get(key) is not None)
                   for key in sorted(set().union(*(r.keys() for r in values)))}
            for case, values in groups.items()}


def pacing_data(source, cases, repetitions):
    reasons, runs = [], []
    # Strict validation, including repetition identity and common provenance,
    # stays entirely in the original analyzer.
    try:
        records = analyze.load(source, cases, repetitions)
    except (ValueError, OSError, KeyError, TypeError, OverflowError, csv.Error) as error:
        records = []
        reasons.append(str(error))
    paths = sorted(source.glob("*.csv")) + sorted(source.glob("*.csv.json"))
    # Keep even rejected sidecars in the evidence inventory; never silently
    # select a valid subset after strict load fails.
    metadata = []
    for path in sorted(source.glob("*.csv.json")):
        try:
            record = json.loads(path.read_text())
            metadata.append({"file": path.name, "metadata": record.get("metadata")})
        except (ValueError, OSError, AttributeError):
            metadata.append({"file": path.name, "metadata": None})
    series = []
    for record in records:
        window_metrics, run_series = throughput.windows(record, source)
        metrics = analyze.run_metrics(record)
        if window_metrics:
            metrics["tx_windows"] = window_metrics
        series.extend(run_series)
        runs.append({"file": record["csv"], "case": record["case"],
                     "metadata": record["metadata"], "phases": record["phases"],
                     "validity": {"status": "valid", "reasons": []},
                     "metrics": metrics})
    return runs, reasons, paths, {"raw_sidecars": metadata,
                                  "expected_cases": cases, "expected_repetitions": repetitions}, series


def udp_data(source, expected_delay_ms, out, min_packets=1, expected_csv=()):
    from measurements.udp import parse_rows, check_common, owd_summary

    paths = (sorted(path for path in source.rglob("*.csv")
                    if not (out.resolve() != source.resolve() and path.resolve().is_relative_to(out.resolve()))
                    and path.name != "summary.csv") if source.is_dir() else [source])
    reasons, runs = [], []
    discovered = {str(path.relative_to(source)) if source.is_dir() else path.name for path in paths}
    if expected_csv and discovered != set(expected_csv):
        reasons.append(f"CSV artifact set differs: expected {sorted(expected_csv)}, found {sorted(discovered)}")
    if not paths:
        reasons.append(f"{source}: no UDP CSV artifacts")
    for path in paths:
        errors, metrics = [], {}
        try:
            if path.name == "summary.csv":
                raise ValueError("generated summary.csv is not raw UDP input")
            rows = parse_rows(path, errors)
            check_common(path, rows, min_packets, errors)
            if not any(not row.lost for row in rows):
                errors.append(f"{path}: no received packets")
            # Invalid rows are retained on disk, but not summarized as valid data.
            if not errors:
                metrics = owd_summary(rows, expected_delay_ms=expected_delay_ms)
        except (ValueError, OSError, KeyError, TypeError, OverflowError) as error:
            errors.append(f"{path}: {error}")
        label = str(path.relative_to(source)) if source.is_dir() else path.name
        runs.append({"file": label, "case": "udp", "metrics": metrics,
                     "validity": {"status": "invalid" if errors else "valid", "reasons": errors}})
        reasons.extend(errors)
    run_metadata = []
    metadata_paths = sorted(source.rglob("run.json")) if source.is_dir() else []
    for candidate in dict.fromkeys([*metadata_paths, source.parent / "run.json"]):
        if candidate.is_file():
            try:
                run_metadata.append({"path": str(candidate.resolve()), "data": json.loads(candidate.read_text())})
                paths.append(candidate)
            except (OSError, ValueError) as error:
                reasons.append(f"{candidate}: invalid run provenance: {error}")
    builds = []
    # Preserve actual endpoint binary identity when the nixnet lifecycle saved it.
    parents = {source.parent, *(path.parent for path in metadata_paths)}
    for parent in sorted(parents):
        for node in ("client", "server"):
            build, digest = parent / node / "build.txt", parent / node / "binary.sha256"
            if build.is_file() and digest.is_file():
                paths.extend([build, digest])
                try:
                    fields = digest.read_text().split()
                    sha = fields[0] if fields else ""
                    version = build.read_text().strip()
                    if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha) or not version:
                        raise ValueError("empty version or invalid binary SHA256")
                    builds.append({"node": node, "directory": str(parent.resolve()),
                                   "version": version, "binary_sha256": sha})
                except (OSError, ValueError) as error:
                    reasons.append(f"{parent}/{node}: invalid build provenance: {error}")
    return runs, reasons, paths, {"input": str(source.resolve()),
                                  "expected_delay_ms": expected_delay_ms,
                                  "expected_csv": list(expected_csv), "min_packets": min_packets,
                                  "run_metadata": run_metadata,
                                  "endpoint_builds": builds,
                                 "source_provenance": "unknown; inspect unverified run_metadata" if run_metadata else "unknown",
                                 "invocation_provenance": "unknown; inspect unverified run_metadata" if run_metadata else "unknown"}


def timestamp_provenance(logs):
    if not logs:
        return {"source": "unknown", "warnings": ["No TX logs supplied; timestamp source is unverified."]}, []
    errors = []
    for path in logs:
        try:
            text = Path(path).read_text()
            if "selectMsgsSent: returning with txTsReader infos" not in text:
                errors.append(f"{path}: missing positive kernel TX timestamp marker")
            for marker in ("Failed enabling tx timestamping", "using timestamps from run function",
                           "TxTsReader was not used, use sentMsgs from Run func"):
                if marker.lower() in text.lower():
                    errors.append(f"{path}: timestamp fallback marker: {marker}")
        except OSError as error:
            errors.append(f"{path}: {error}")
    return {"source": "unknown" if errors else "kernel software TX",
            "logs": inventory([Path(path) for path in logs])}, errors


def plot(runs, suite):
    """Per-run dots; UDP ranges expose extrema, not a clipped pooled sample."""
    points = []
    for run in runs:
        if run["validity"]["status"] != "valid":
            continue
        metrics = run["metrics"]
        values = ([metrics["owd_ms"][key] for key in ("min", "median", "p99", "max")]
                  if suite == "udp" else [metrics["gap_median_us"], metrics["gap_p99_us"]])
        points.append((run["file"], [v for v in values if v is not None]))
    values = [v for _, group in points for v in group]
    low, high = min(values, default=0), max(values, default=1)
    span = high - low or 1
    height = max(100, 60 + 28 * len(points))
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="720" height="{height}" viewBox="0 0 720 {height}">',
           '<rect width="720" height="100%" fill="white"/>',
           '<g font-family="sans-serif" font-size="12" fill="#222">',
           '<text x="10" y="18">' + ("OWD ms: min, median, p99, max (each CSV)" if suite == "udp"
                                                   else "TX gap µs: median, p99 (each run)") + '</text>',
           f'<text x="250" y="38">{low:.6g}</text><text x="640" y="38">{high:.6g}</text>']
    for i, (label, group) in enumerate(points):
        y = 58 + i * 28
        svg.append(f'<text x="10" y="{y}">{escape(label[:32])}<title>{escape(label)}</title></text>')
        xs = [250 + (v - low) / span * 430 for v in group]
        if xs:
            svg.append(f'<path d="M {min(xs)} {y-4} H {max(xs)}" stroke="#777"/>')
        for x, value in zip(xs, group):
            svg.append(f'<circle cx="{x}" cy="{y-4}" r="3" fill="#2166ac"><title>{value:.9g}</title></circle>')
    if not points:
        svg.append('<text x="10" y="65">No valid runs to plot.</text>')
    return "\n".join(svg + ["</g></svg>\n"])


def generate(suite, source, out, *, cases=None, repetitions=6, expected_delay_ms=None,
             execution=None, provenance=None, backend=None, tx_logs=(), min_packets=1, expected_csv=()):
    source, out = Path(source), Path(out)
    if source.is_dir() and source.resolve() == out.resolve():
        raise ValueError("report output would overwrite the input directory; choose a different --out directory")
    if source.is_file() and source.resolve() in {(out / name).resolve() for name in
                                                ("manifest.json", "metrics.json", "summary.csv", "report.md", "plot.svg",
                                                 "throughput.csv", "throughput.svg")}:
        raise ValueError("report output would overwrite the input; choose a different --out directory")
    if suite == "pacing":
        runs, reasons, paths, raw_provenance, series = pacing_data(source, cases or list(analyze.CASES), repetitions)
        raw_provenance["tx_windows"] = {
            "anchor": "metadata.start_unix_ns", "interval": "half-open [start,end), complete windows within nominal 1200ms Run budget",
            "window_ms": list(throughput.WINDOWS_MS), "udp_bytes": 1408,
            "cases": ["single100", "single500"],
            "outlier_definition": "absolute deviation >10% of integer-pps scheduled UDP Mbps; descriptive only, not acceptance",
            "cv_definition": "population standard deviation / mean across complete windows, including zero-traffic windows"}
        backend = backend or "linux-loopback-kernel-software-tx"
    else:
        runs, reasons, paths, raw_provenance = udp_data(source, expected_delay_ms, out, min_packets, expected_csv)
        backend = backend or "external-udp-csv"
        timestamps, errors = timestamp_provenance(tx_logs)
        raw_provenance["timestamps"] = timestamps
        reasons.extend(errors)
        paths.extend(Path(path) for path in tx_logs)
    execution = execution or {"status": "unknown", "reason": "Independent artifacts do not prove execution completion."}
    summary = aggregate(runs)
    outputs = ["manifest.json", "metrics.json", "summary.csv", "report.md", "plot.svg"]
    if suite == "pacing":
        outputs += ["throughput.csv", "throughput.svg"]
    collisions = {path.resolve() for path in paths} & {(out / name).resolve() for name in outputs}
    if collisions:
        raise ValueError("report output would overwrite the input: " + ", ".join(map(str, sorted(collisions))))
    out.mkdir(parents=True, exist_ok=True)
    if suite == "pacing":
        throughput.write(out, series)
    manifest = {"schema_version": 1, "suite": suite, "backend": backend,
                "execution": execution,
                "validity": {"status": "invalid" if reasons else "valid", "reasons": reasons},
                "acceptance": {"status": "not_evaluated", "reason": "No performance thresholds are applied."},
                "artifacts": {"reports": outputs, "raw": inventory(paths)},
                "provenance": {**raw_provenance, **(provenance or {})}}
    write_json(out / "manifest.json", manifest)
    write_json(out / "metrics.json", {"schema_version": 1, "suite": suite, "runs": runs,
                                     "aggregate": summary})
    with (out / "summary.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["scope", "case", "file", "metric", "value", "median", "min", "max", "n", "validity"])
        for run in runs:
            values = flatten(run["metrics"])
            if not values:
                writer.writerow(["run", run["case"], run["file"], "", "", "", "", "", "", run["validity"]["status"]])
            for key, value in values.items():
                writer.writerow(["run", run["case"], run["file"], key, value, "", "", "", "", run["validity"]["status"]])
        for case, metrics in summary.items():
            for key, d in metrics.items():
                writer.writerow(["aggregate", case, "", key, "", d["median"], d["min"], d["max"], d["n"], "valid"])
    lines = [f"# {suite} report", "", f"Execution: **{execution['status']}**. ",
             f"Artifact validity: **{manifest['validity']['status']}**. Acceptance: **not_evaluated**.", "",
             "No performance thresholds. Validity is structural, not a performance verdict.",
             "Per-run evidence is retained; aggregates are median/min/max of run metrics, not pooled packets.", ""]
    lines += ["- " + reason.replace("\n", " ") for reason in reasons]
    if suite == "udp":
        lines += ["", "Timestamp source: " + raw_provenance["timestamps"]["source"] + "."]
        lines += raw_provenance["timestamps"].get("warnings", [])
        if not raw_provenance["run_metadata"]:
            lines += ["Source/invocation provenance: unknown (no run.json found alongside input)."]
    lines += ["", "| Case | File | Validity |", "| --- | --- | --- |"]
    lines += [f"| {r['case']} | {r['file'].replace('|', '&#124;')} | {r['validity']['status']} |" for r in runs]
    lines += ["", "| Case | Metric | Median | Min | Max | n |", "| --- | --- | ---: | ---: | ---: | ---: |"]
    for case, metrics in summary.items():
        for key, d in metrics.items():
            lines.append(f"| {case} | {key} | {d['median']} | {d['min']} | {d['max']} | {d['n']} |")
    lines += ["", "![Per-run measurements](plot.svg)", "", "See metrics.json for full per-run metrics and manifest.json for provenance.", ""]
    if suite == "pacing":
        lines += ["## Continuous-control throughput", "",
                  "Run-start anchored complete half-open 1/5/10/20/60 ms windows within the nominal 1200 ms budget; 1408 UDP bytes per packet.",
                  "CV is population standard deviation / mean, including zero-traffic windows. Outliers deviate >10% from the integer-pps scheduled target; descriptive only, not an acceptance threshold.",
                  "Dynamic schedules are excluded because their gaps/rate changes are intentional. All windows are in throughput.csv; no clipping or outlier removal.",
                  "", "![20 ms throughput per continuous run](throughput.svg)", ""]
    (out / "report.md").write_text("\n".join(lines))
    (out / "plot.svg").write_text(plot(runs, suite))
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="suite", required=True)
    pacing = sub.add_parser("pacing")
    pacing.add_argument("input", type=Path)
    pacing.add_argument("--out", required=True, type=Path)
    pacing.add_argument("--repetitions", type=int, default=6)
    pacing.add_argument("--cases", type=cases_argument, default=list(analyze.CASES))
    udp = sub.add_parser("udp")
    udp.add_argument("input", type=Path)
    udp.add_argument("--out", required=True, type=Path)
    udp.add_argument("--expected-delay-ms", type=float)
    udp.add_argument("--execution-status", choices=["complete", "failed", "unknown"], default="unknown",
                     help="caller-supplied execution evidence; offline default is unknown")
    udp.add_argument("--backend", default="external-udp-csv")
    udp.add_argument("--tx-log", action="append", type=Path, default=[])
    udp.add_argument("--min-packets", type=int, default=1, help="required rows in every raw CSV")
    udp.add_argument("--expect-csv", action="append", default=[], help="exact expected relative CSV paths (repeatable)")
    args = parser.parse_args(argv)
    if args.suite == "pacing" and args.repetitions < 1:
        parser.error("repetitions must be positive")
    if args.suite == "udp" and args.expected_delay_ms is not None and (
            not math.isfinite(args.expected_delay_ms) or args.expected_delay_ms < 0):
        parser.error("expected delay must be finite and nonnegative")
    if args.suite == "udp" and args.min_packets < 1:
        parser.error("min-packets must be positive")
    kwargs = ({"cases": args.cases, "repetitions": args.repetitions} if args.suite == "pacing"
              else {"expected_delay_ms": args.expected_delay_ms, "backend": args.backend,
                     "tx_logs": args.tx_log,
                     "min_packets": args.min_packets, "expected_csv": args.expect_csv,
                    "execution": {"status": args.execution_status,
                                  "reason": "Caller-supplied status" if args.execution_status != "unknown"
                                  else "Independent artifacts do not prove execution completion."}})
    try:
        result = generate(args.suite, args.input, args.out, **kwargs)
    except (OSError, ValueError) as error:
        parser.exit(1, f"error: {error}\n")
    return 0 if result["validity"]["status"] == "valid" and result["execution"]["status"] != "failed" else 1


if __name__ == "__main__":
    sys.exit(main())
