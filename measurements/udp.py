"""Parse and validate the UDP CSV emitted by NetScalpel (stdlib only)."""

import csv
import math
import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class Row:
    seq: int
    ts_sent: datetime
    ts_rcvd: datetime | None
    owd_ms: float | None
    size: int
    lost: bool
    ts_sent_ns: int
    ts_rcvd_ns: int | None


_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.(\d{1,9}))?(?:Z|[+-](\d{2}):(\d{2}))"
)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_timestamp(value: str) -> tuple[datetime, int]:
    """Return an aware datetime and exact epoch nanoseconds, without floats."""
    match = _TIMESTAMP.fullmatch(value)
    if not match or (match[2] and (int(match[2]) > 23 or int(match[3]) > 59)):
        raise ValueError(f"invalid RFC3339 timestamp: {value!r}")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    delta = dt.astimezone(timezone.utc) - _EPOCH
    ns = (delta.days * 86400 + delta.seconds) * 1_000_000_000
    ns += int((match[1] or "").ljust(9, "0"))
    return dt, ns


def parse_rows(path: str, errors: list[str]) -> list[Row]:
    rows = []
    try:
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            expected = ["seq", "ts_sent", "ts_rcvd", "owd_ms", "size", "lost", "dup_count"]
            fields = reader.fieldnames or []
            extras = [f"ts_rcvd_{i}" for i in range(1, len(fields) - len(expected) + 1)]
            if fields != expected + extras:
                errors.append(f"{path}: header {fields} != {expected + extras}")
                return rows
            max_duplicates = 0
            for raw in reader:
                where = f"{path}: line {reader.line_num}"
                if None in raw or any(v is None for v in raw.values()):
                    errors.append(f"{where}: row width does not match header")
                    continue
                try:
                    sent, sent_ns = parse_timestamp(raw["ts_sent"])
                    received, received_ns = (
                        parse_timestamp(raw["ts_rcvd"]) if raw["ts_rcvd"] else (None, None)
                    )
                    row = Row(int(raw["seq"]), sent, received,
                              float(raw["owd_ms"]) if raw["owd_ms"] else None,
                              int(raw["size"]), raw["lost"] == "true", sent_ns, received_ns)
                    dup_count = int(raw["dup_count"])
                    duplicates = [parse_timestamp(raw[k])[1] if raw[k] else None for k in extras]
                    if raw["lost"] not in ("true", "false"):
                        raise ValueError("lost must be true or false")
                    if row.seq < 0 or row.size < 0:
                        raise ValueError("seq and size must be nonnegative")
                    if row.owd_ms is not None and (not math.isfinite(row.owd_ms) or row.owd_ms < 0):
                        raise ValueError("owd_ms must be finite and nonnegative")
                    if not 0 <= dup_count <= len(extras):
                        raise ValueError(f"dup_count {dup_count} outside [0, {len(extras)}]")
                    max_duplicates = max(max_duplicates, dup_count)
                    if any(t is None for t in duplicates[:dup_count]) or any(
                        t is not None for t in duplicates[dup_count:]
                    ):
                        raise ValueError("duplicate timestamps do not match dup_count")
                    if row.lost and (dup_count or any(t is not None for t in duplicates)):
                        raise ValueError("lost row has duplicate copies")
                    arrivals = [t for t in [received_ns, *duplicates] if t is not None]
                    if any(a > b for a, b in zip(arrivals, arrivals[1:])):
                        raise ValueError("receive timestamps are not chronological")
                    if row.lost and (received is not None or row.owd_ms is not None):
                        raise ValueError("lost row has ts_rcvd/owd_ms set")
                    if not row.lost:
                        if received_ns is None or row.owd_ms is None:
                            raise ValueError("received row is missing ts_rcvd/owd_ms")
                        elapsed_ms = (received_ns - sent_ns) / 1_000_000
                        # fmtDurationMs emits full float precision, not rounded milliseconds.
                        if elapsed_ms < 0 or not math.isclose(row.owd_ms, elapsed_ms, rel_tol=1e-12, abs_tol=1e-6):
                            raise ValueError(f"owd_ms {row.owd_ms} disagrees with timestamps ({elapsed_ms} ms)")
                    rows.append(row)
                except (ValueError, OverflowError) as exc:
                    errors.append(f"{where}: invalid value: {exc}")
            if len(extras) != max_duplicates:
                errors.append(f"{path}: {len(extras)} duplicate timestamp columns, but maximum dup_count is {max_duplicates}")
    except (OSError, csv.Error, UnicodeError) as exc:
        errors.append(f"{path}: cannot read CSV: {exc}")
    return rows


def check_common(path: str, rows: list[Row], min_packets: int, errors: list[str]):
    """Check sequence completeness and nondecreasing TX times at ns precision.

    Equal software TX timestamps are legitimate for packets in the same batch.
    """
    if not rows or len(rows) < min_packets:
        errors.append(f"{path}: only {len(rows)} rows, expected >= {max(1, min_packets)}")
    if rows and rows[0].seq != 0:
        errors.append(f"{path}: seq starts at {rows[0].seq}, expected 0")
    for prev, cur in zip(rows, rows[1:]):
        if cur.seq != prev.seq + 1:
            errors.append(f"{path}: seq jumps from {prev.seq} to {cur.seq}")
        if cur.ts_sent_ns < prev.ts_sent_ns:
            errors.append(f"{path}: ts_sent moves backwards at seq {cur.seq}")


def quantile(values: list[float], q: float) -> float:
    """Nearest-rank quantile, matching the pacing analyzer's convention."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * q) - 1)]


def _distribution(values: list[float]) -> dict:
    if not values:
        return dict.fromkeys(("min", "median", "p90", "p99", "max"))
    return {"min": min(values), "median": statistics.median(values),
            "p90": quantile(values, .9), "p99": quantile(values, .99), "max": max(values)}


def owd_summary(rows: list[Row], expected_delay_ms: float | None = None) -> dict:
    """JSON-safe counts and distributions; error_ms is signed measured - expected."""
    if expected_delay_ms is not None and (not math.isfinite(expected_delay_ms) or expected_delay_ms < 0):
        raise ValueError("expected_delay_ms must be finite and nonnegative")
    values = [r.owd_ms for r in rows if not r.lost and r.owd_ms is not None]
    lost = sum(r.lost for r in rows)
    result = {"packets": len(rows), "received": len(values), "lost": lost,
              "loss_percent": 100 * lost / len(rows) if rows else None,
              "owd_ms": _distribution(values)}
    if expected_delay_ms is not None:
        result["error_ms"] = _distribution([v - expected_delay_ms for v in values])
    return result
