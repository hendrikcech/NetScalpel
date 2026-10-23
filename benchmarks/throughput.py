"""Descriptive Run-start-anchored kernel TX windows for continuous controls."""
import csv
from html import escape
import statistics

WINDOWS_MS = (1, 5, 10, 20, 60)
OUTLIER_RELATIVE_DEVIATION = .10


def windows(record, directory):
    if record["case"] not in ("single100", "single500"):
        return {}, []
    duration_ns = sum(p["duration_ms"] for p in record["phases"]) * 1_000_000
    start_ns = record["metadata"]["start_unix_ns"]
    target_pps = record["phases"][0]["pps"]
    target_mbps = target_pps * 1408 * 8 / 1_000_000
    with (directory / record["csv"]).open(newline="") as stream:
        elapsed = [int(row["kernel_tx_unix_ns"]) - start_ns for row in csv.DictReader(stream)]
    result, series = {}, []
    for ms in WINDOWS_MS:
        width_ns = ms * 1_000_000
        counts = [0] * (duration_ns // width_ns)
        # Half-open windows [start, end). Never include a partial final window.
        for timestamp in elapsed:
            if 0 <= timestamp < len(counts) * width_ns:
                counts[timestamp // width_ns] += 1
        rates = [count * 1408 * 8 / (ms * 1000) for count in counts]
        mean = statistics.mean(rates)
        sd = statistics.pstdev(rates)
        result[f"{ms}ms"] = {"windows": len(counts), "mean_mbps": mean,
                              "min_mbps": min(rates), "max_mbps": max(rates),
                              "stddev_mbps": sd, "cv": sd / mean if mean else None,
                              "target_mbps": target_mbps,
                              "outlier_percent": 100 * sum(abs(rate - target_mbps) > target_mbps * OUTLIER_RELATIVE_DEVIATION
                                                           for rate in rates) / len(rates)}
        for i, (count, rate) in enumerate(zip(counts, rates)):
            series.append({"case": record["case"], "file": record["csv"], "window_ms": ms,
                           "start_ms": i * ms, "end_ms": (i + 1) * ms,
                           "packets": count, "udp_mbps": rate, "target_mbps": target_mbps})
    return result, series


def write(out, series):
    with (out / "throughput.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["case", "file", "window_ms", "start_ms", "end_ms",
                                                    "packets", "udp_mbps", "target_mbps"])
        writer.writeheader()
        writer.writerows(series)
    groups = {}
    for row in series:
        if row["window_ms"] == 20:
            groups.setdefault(row["file"], []).append(row)
    height = max(100, 190 * len(groups))
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="720" height="{height}" viewBox="0 0 720 {height}">',
           '<rect width="720" height="100%" fill="white"/>',
           '<g font-family="sans-serif" font-size="12" fill="#222">']
    for i, (name, rows) in enumerate(groups.items()):
        top = i * 190
        maximum = max([r["udp_mbps"] for r in rows] + [rows[0]["target_mbps"]]) or 1
        svg += [f'<text x="10" y="{top+18}">{escape(name)}: 20 ms UDP Mbps</text>',
                f'<text x="10" y="{top+38}">y: 0–{maximum:.5g}; x: 0–1200 ms after Run start</text>']
        points = " ".join(f'{50 + r["start_ms"] / 1200 * 650:.3f},{top + 160 - r["udp_mbps"] / maximum * 110:.3f}'
                          for r in rows)
        target_y = top + 160 - rows[0]["target_mbps"] / maximum * 110
        svg += [f'<path d="M 50 {top+50} V {top+160} H 700" fill="none" stroke="#888"/>',
                f'<path d="M 50 {target_y} H 700" stroke="#999" stroke-dasharray="4 4"><title>Integer-pps scheduled target</title></path>',
                f'<polyline points="{points}" fill="none" stroke="#2166ac" stroke-width="1.5"/>']
    if not groups:
        svg.append('<text x="10" y="30">No valid single100/single500 controls to plot.</text>')
    (out / "throughput.svg").write_text("\n".join(svg + ["</g></svg>\n"]))
