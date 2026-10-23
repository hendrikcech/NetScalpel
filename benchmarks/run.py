#!/usr/bin/env python3
"""Run the real sequential Go pacing suite into a fresh artifact directory."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmarks import analyze, report


def run_pacing(out, repetitions, cases):
    out = Path(out).resolve()
    # Atomic creation is intentional: never reuse even an empty existing path.
    out.mkdir(parents=True, exist_ok=False)
    raw = out / "raw"
    raw.mkdir()
    bench = "^BenchmarkRateSender$"
    if set(cases) != set(analyze.CASES):
        bench += "/^(" + "|".join(cases) + ")$"
    command = ["go", "test", "./pkg", "-run", "^$", "-bench", bench,
               "-benchtime=1x", "-count", str(repetitions), "-timeout=10m"]
    env = os.environ.copy()
    env["NETSCALPEL_PACING_STUDY"] = str(raw)
    # An inherited single-case filter must not silently narrow the requested set.
    env.pop("NETSCALPEL_PACING_CASE", None)
    started = datetime.now(timezone.utc).isoformat()
    invocation = {"argv": command, "cwd": str(ROOT), "started_utc": started,
                  "environment": {key: value for key, value in env.items()
                                  if key.startswith("NETSCALPEL_PACING_") or key in
                                  ("GOFLAGS", "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOOS", "GOARCH", "CGO_ENABLED")}}
    report.write_json(out / "invocation.json", invocation)
    execution = {"status": "failed", "started_utc": started}
    with (out / "go-bench.txt").open("w") as log:
        try:
            process = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
            execution.update(status="complete" if process.returncode == 0 else "failed",
                             returncode=process.returncode)
        except (OSError, KeyboardInterrupt) as error:
            execution["reason"] = str(error) or type(error).__name__
            log.write(execution["reason"] + "\n")
    execution["finished_utc"] = datetime.now(timezone.utc).isoformat()
    report.write_json(out / "execution.json", execution)
    destination = out / "report"
    result = report.generate("pacing", raw, destination, cases=cases, repetitions=repetitions,
                             execution=execution, provenance={"invocation": invocation})
    result["artifacts"]["runner"] = report.inventory([out / name for name in
                                                       ("invocation.json", "execution.json", "go-bench.txt")])
    report.write_json(destination / "manifest.json", result)
    return 0 if execution["status"] == "complete" and result["validity"]["status"] == "valid" else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=["pacing"])
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--repetitions", default=6, type=int)
    parser.add_argument("--cases", type=report.cases_argument, default=list(analyze.CASES))
    args = parser.parse_args(argv)
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    try:
        return run_pacing(args.out, args.repetitions, args.cases)
    except FileExistsError:
        parser.exit(1, f"error: output path already exists: {args.out}\n")


if __name__ == "__main__":
    sys.exit(main())
