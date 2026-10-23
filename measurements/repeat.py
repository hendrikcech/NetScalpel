#!/usr/bin/env python3
"""Run indexed measurements without hiding failed repetition exit codes."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


def run_range(command, indices, status_path, fresh_root=None):
    if fresh_root is not None:
        Path(fresh_root).mkdir(parents=True, exist_ok=False)
    status_path = Path(status_path)
    status_path.parent.mkdir(parents=True, exist_ok=True)
    record = {"schema_version": 1, "status": "running", "runs": []}

    def save():
        temporary = status_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(record, indent=2) + "\n")
        temporary.replace(status_path)

    save()
    for index in indices:
        invocation = [*command, str(index)]
        run = {"index": index, "argv": invocation, "status": "failed"}
        try:
            process = subprocess.run(invocation, check=False)
            run["returncode"] = process.returncode
            run["status"] = "complete" if process.returncode == 0 else "failed"
        except OSError as error:
            run["error"] = str(error)
        record["runs"].append(run)
        save()
        if run["status"] != "complete":
            print(f"repetition {index} failed: {run.get('error', 'exit ' + str(run.get('returncode')))}", file=sys.stderr)
    record["status"] = "complete" if record["runs"] and all(run["status"] == "complete" for run in record["runs"]) else "failed"
    save()
    return 0 if record["status"] == "complete" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--range", default="0", help="one index or inclusive first-last")
    parser.add_argument("--status", required=True, type=Path)
    parser.add_argument("--fresh-root", type=Path, help="atomically reserve a new output root; refuse stale data")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?", args.range) or not command:
        parser.error("requires a nonnegative index/range and a command after --")
    bounds = [int(value) for value in args.range.split("-")]
    first, last = bounds[0], bounds[-1]
    if last < first:
        parser.error("last index must be >= first index")
    try:
        return run_range(command, range(first, last + 1), args.status, args.fresh_root)
    except FileExistsError:
        parser.exit(1, f"error: output path already exists: {args.fresh_root}\n")


if __name__ == "__main__":
    raise SystemExit(main())
