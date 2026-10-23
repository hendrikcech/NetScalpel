# Measurement benchmarks and reports

## Supported architecture

The supported measurement backends are the real Go `RateSender` benchmark on
Linux loopback and the Nix/nixnet `benchmark-owd` experiment (20 ms UDP uplink and
downlink). Python 3.11+ reporting uses only the standard library. There is no
remote-host runner, hardware timestamp backend, or performance acceptance gate.
Loopback kernel software TX measurements are **not NIC/on-wire pacing**.

```sh
# Sequential real Go benchmark; OUT must not already exist, even if empty.
python3 benchmarks/run.py pacing --out out/benchmark-pacing --repetitions 3
# Intentional subset, exact sub-benchmark matching (no concurrent workloads):
python3 benchmarks/run.py pacing --out out/pacing-low --repetitions 2 \
  --cases single7pps,split7pps,short7pps

# Separate supported Nix backend (requires Nix and the nixnet runtime):
nix run ./e2e#benchmark-owd

# Offline reports never infer successful execution from the presence of files.
python3 benchmarks/report.py pacing out/benchmark-pacing/raw \
  --out out/pacing-offline --repetitions 3
python3 benchmarks/report.py udp out/benchmark-owd/RUN/client \
  --out out/owd-offline --expected-delay-ms 20 \
  --min-packets 250 --expect-csv owd_ul.csv --expect-csv owd_dl.csv
```

The pacing runner writes `raw/*.csv`, matching sidecars, `invocation.json`,
`execution.json`, and combined stdout/stderr in `go-bench.txt`. Reports live in
`OUT/report/`: `manifest.json`, `metrics.json`, `summary.csv`, `report.md`, and
`plot.svg`; pacing also writes `throughput.csv` and `throughput.svg`. The runner
clears inherited `NETSCALPEL_PACING_CASE` and selects the requested cases exactly.
Other pacing environment settings are preserved and recorded. A failed Go launch,
failed benchmark, or invalid/incomplete artifact set still generates reports and
returns nonzero. The existing strict `analyze.py` remains the source of pacing
validation/comparison semantics; it additionally rejects orphan raw CSVs without
sidecars, including interrupted partial iterations.

### Report contract

`manifest.json` and `metrics.json` have `schema_version: 1`. The manifest separates
execution (`complete`, `failed`, `unknown`), structural validity (`valid`,
`invalid`, with reasons), and acceptance (`not_evaluated`). It inventories raw
artifacts with SHA-256 and retains each pacing sidecar's metadata. JSON keeps
per-run metrics; CSV includes per-run rows and median/min/max/sample-count
aggregates. Markdown includes status, errors, metric tables, and SVG plots. An
empty directory is invalid, not vacuously successful. Raw data/outliers are never
removed, and repetitions are never pooled into a single packet population.

UDP input is one CSV or a directory recursively containing CSVs. Every raw CSV
is checked independently and keeps its relative filename, including files with
the same basename in different directories. Report output subtrees and generated
`summary.csv` files are excluded from recursive discovery. UDP validity checks
structure and requires some received packets; **no OWD distribution, loss-rate,
or measured-minus-expected error threshold is imposed**. `--expected-delay-ms`
adds signed error distributions. An available `run.json` beside/above the input
is retained as provenance; absent source/invocation provenance is unknown.
`--min-packets` sets the minimum rows per file, and repeated `--expect-csv`
declares the exact relative artifact set. The nixnet suite requires both UL/DL
files and at least 250 rows in each, rather than accepting partial discovery.
Endpoint build versions and binary SHA256 files saved by nixnet are retained.
OWD p90/p99 use the nearest-rank convention, matching the pacing analyzer.

Offline execution defaults to `unknown`. Only a caller with external execution
evidence should supply UDP `--execution-status complete` (or `failed`). Nixnet's
postRun uses `--backend nixnet` and repeated `--tx-log PATH` arguments. Each
supplied TX log must contain `selectMsgsSent: returning with txTsReader infos`
and no timestamp fallback marker. Missing logs, missing positive markers, and
fallbacks make validity invalid **without suppressing the report**. Without
`--tx-log`, timestamp provenance is explicitly unknown with a warning; the
report does not claim kernel timestamps. Backend labels alone prove nothing.

### Continuous pacing stability

For validated `single100`/`single500` controls, `throughput.csv` contains every
complete half-open window anchored to the recorded Run-start integer nanoseconds,
with widths **1, 5, 10, 20, 60 ms** inside the nominal 1200 ms measurement budget.
Partial final windows are excluded. Rates count 1408 UDP bytes per packet; target
rates use the schedule's integer packets/second, not a rounded Mbps label.
Per-run metrics include mean/min/max, population standard deviation, CV
(standard deviation / mean), and percentage of windows deviating **more than
10%** from that target. This descriptive cutoff is not an acceptance criterion;
short windows naturally expose packet-count quantization. Zero-traffic windows
are included; zero overall mean yields null CV. The SVG shows each run's 20 ms
time series without clipping. Dynamic schedules are excluded from constant-rate
window analysis because their cooldowns and rate changes are intentional.

## RateSender pacing details

Linux loopback measurements of the real `RateSender.Run` path, not latency
assertions. The suite requires **complete kernel software TX timestamps** and
fails rather than accepting the sender's application-timestamp fallback. Each
socket requests a 30 MB receive buffer for the TX error queue; check the host's
`net.core.rmem_max` if completeness fails. Do not reinterpret failed runs as data.

## Run

From the repository root, on an otherwise idle host:

```sh
mkdir -p /tmp/opencode/pacing-after
NETSCALPEL_PACING_STUDY=/tmp/opencode/pacing-after \
NETSCALPEL_PACING_LABEL=anchored \
go test ./pkg -run '^$' -bench '^BenchmarkRateSender$' -benchtime=1x -count=6 \
  -timeout=10m > /tmp/opencode/pacing-after/go-bench.txt
```

This runs each schedule exactly once per repetition; do not use the usual
time-calibrated `-benchtime=1s`. Use `-count` to choose the fixed number of
repetitions. Other benchtime settings may include Go calibration probes, which
also write artifacts and cause strict repetition-count validation to fail.
No opt-in is needed to run the benchmarks; omitting
`NETSCALPEL_PACING_STUDY` disables artifact output only. Use fresh output
directories, since artifacts are append-only and never overwritten.

Do **not** run `-race`, `b.RunParallel`, concurrent benchmarks, background builds,
or other tests during measurements. The suite itself is sequential; global slog
observation deliberately does not support concurrent workloads. Go runs
repetitions grouped by case; order is fixed and recorded in the invocation,
not rotated as in the original exploratory test. Alternate before/after run
order for follow-up checks when thermal/order effects are a concern. Do not
select a subset accidentally: use an exact Go sub-benchmark regexp, or
`NETSCALPEL_PACING_CASE=single7pps` (unknown names fail explicitly).

Each iteration saves a uniquely named `<case>-*.csv` and matching
`<case>-*.csv.json`. CSV columns are sequence, kernel TX Unix nanoseconds,
elapsed nanoseconds from Run entry, original segment index, and UDP bytes.
JSON includes every scheduled segment, including cooldowns and unobserved
segments, packet counts, rate, span, drift, boundary gaps, jitter, raw CSV
basename and metadata. Metadata captures suite/schema version, Git revision and
dirty status, production and benchmark source SHA-256, Go version, OS/architecture, kernel,
CPU/GOMAXPROCS, Go tuning environment, label, timestamp source, deadline mode, and command-line args.
Record any external CPU affinity/governor configuration alongside the run.

### Compare unchanged baseline source with a Go overlay

Use the **same benchmark file and dependencies** for both versions. An overlay
can replace only production `pkg/udp.go`; no benchmark duplication is needed:

```sh
python3 - <<'PY'
import json
from pathlib import Path
empty = Path('/tmp/opencode/pacing-empty-tests.go')
empty.write_text('package pkg\n')
replace = {str(Path('pkg/udp.go').resolve()): '/tmp/opencode/udp-before-issue26.go'}
# Go compiles test files even with -run '^$'. This new unit-test file
# references new-only helpers; exclude it from the baseline build, not the suite.
timing_tests = Path('pkg/udp_rate_timing_test.go')
if timing_tests.exists():
    replace[str(timing_tests.resolve())] = str(empty)
Path('/tmp/opencode/pacing-before-overlay.json').write_text(json.dumps({
    'Replace': replace
}))
PY
mkdir -p /tmp/opencode/pacing-before
NETSCALPEL_PACING_STUDY=/tmp/opencode/pacing-before \
NETSCALPEL_PACING_LABEL=before-issue26 \
NETSCALPEL_PACING_SOURCE=/tmp/opencode/udp-before-issue26.go \
NETSCALPEL_PACING_LEGACY_PREFIX=1 \
go test -overlay=/tmp/opencode/pacing-before-overlay.json ./pkg -run '^$' \
  -bench '^BenchmarkRateSender$' -benchtime=1x -count=6 -timeout=10m \
  > /tmp/opencode/pacing-before/go-bench.txt
```

`NETSCALPEL_PACING_SOURCE` **must identify the compiled production source** when
using an overlay or prebuilt test binary: runtime filesystem reads are not
redirected by Go overlays. Default is `udp.go` in Go's `pkg/` test working
directory. The source hash, not Git revision alone, identifies dirty/overlaid
production code. This comparison changes only the overlaid source; report any
other production changes/dependency differences separately.

Current production must synchronously emit `"RateSender segment start"` with an
integer `segment` attribute immediately before entering that original scheduled
segment. The test's handler snapshots `sender.seq` there. Indexed observation
allows skipped interior segments without misassigning packets. The unchanged
pre-fix baseline has only `"Dynamic interval calculation"`; the explicit
`NETSCALPEL_PACING_LEGACY_PREFIX=1` switch permits a **known prefix-only loop** to
map those active-segment records. Never enable that switch for a sender that can
skip interior segments. Indexed records take precedence, even if the switch is
set. The hook timestamps differ by the small interval between Run-loop entry
and the legacy interval log; they are segment-entry observations, not packet
timestamps. An absent/malformed indexed observer otherwise fails closed.

## Analyze

```sh
python3 benchmarks/analyze.py /tmp/opencode/pacing-before /tmp/opencode/pacing-after \
  --json /tmp/opencode/pacing-comparison.json
```

Only Python's standard library is used. Output gives each metric's
**median [minimum, maximum] across repetitions**, sample count, and candidate
minus baseline median deltas. JSON includes the same distributions and build
provenance. A single directory also works. By default all 11 cases and exactly
six runs per case are required. For an intentional subset use e.g.
`--cases single7pps,split7pps,short7pps --repetitions 6`.

The analyzer validates raw CSV counts/sequences/packet sizes/timestamps against
JSON, exact integer-nanosecond elapsed times relative to the recorded Run start,
monotonic segment assignments, the exact expected schedules, complete-kernel
status, known cases, repetition counts, and consistent source/runtime provenance
within each directory. It recomputes gap median/p99/population standard deviation,
phase first/last/span/boundary gaps, budget-based achieved rates, and continuous
control virtual-boundary gaps from raw records. Log-derived starts cannot be
independently reconstructed, but must be finite/monotonic, match drift relative to
nominal starts, and imply the recorded budget. Integer ns differences must match
exactly; derived floats use relative tolerance `1e-12`, absolute tolerance `1e-9`
in the metric's units, solely for floating-point roundoff.

Every CSV reference and Run start must be unique, run identities combine creation
timestamp and Run start, and each sidecar must be named `<csv-basename>.json`.
Copying a sidecar cannot supply an additional repetition. Missing final segments
are reported as unobserved, **never** as zero drift. Metric sample counts expose
missing observations. Extra artifacts,
mixed source builds, unknown cases, or fallback timestamps are errors, not
silently filtered samples. Median deltas are unpaired summaries, not statistical
significance tests.

Before/after environments and accounting must also match: Go/kernel versions,
GOMAXPROCS, CPU count, OS/architecture, Go tuning environment, payload/timestamp
source, suite version, and benchmark-source hash. Normally do not edit the
benchmark between collecting the two sets. For an **intentional** difference,
`--allow-environment-differences` permits comparison with a console warning and
explicit baseline/candidate values under `environment_differences` in JSON.
This override does not waive raw-data validation or permit mixed environments
within a directory; unsupported schemas/accounting still fail validation.
When running a prebuilt test binary, run from
`pkg/` in this same checkout so both source provenance files remain available.

### Cases and interpretation

Original schedules and payload accounting are preserved exactly; `equal100`
adds a symmetric low-rate control:

| Case | Schedule | Outer budget |
| --- | --- | --- |
| `single500`, `single100` | 1200 ms at 500/100 Mbps | 1200 ms |
| `equal500`, `equal100` | 600 ms + 600 ms at the same rate | 1200 ms |
| `up100to500`, `down500to100` | 600 ms per rate | 1200 ms |
| `d2_capped` | 50 × (8 ms at 500 Mbps + 12 ms idle), then 600 ms at 500 Mbps | 1600 ms |
| `d2_uncapped` | identical D2 schedule | 2600 ms (generous, not infinite) |
| `single7pps` | 1200 ms at 7 pps | 1200 ms |
| `split7pps` | 600 ms + 600 ms at 7 pps | 1200 ms |
| `short7pps` | 12 × 100 ms at 7 pps | 1200 ms |

Rates use integer `pps = Mbps * 1_000_000 / 8 / 1408`; padding is 1400 bytes plus
an 8-byte sequence field. These are UDP-data rates, **not** IP/link rates and not
the CLI's 1400-byte nominal-rate denominator. Each 8 ms pulse's original integer
budget is 355 packets (17,750 for 50 pulses). Low-rate 100 ms phases each have a
floor budget of zero packets; no minimum-one-packet correction is applied.

* **Phase drift**: actual segment-entry time minus cumulative nominal start.
  D2 final-phase drift captures cumulative boundary error; compare capped and
  generous-budget runs to separate drift from outer-deadline truncation.
* **Span**: first-to-last kernel TX timestamp in that phase, not its full budget.
  A one-packet phase has zero span; empty/unobserved phases are identified by
  packet count/observed status. JSON retains first and last timestamps relative
  to Run entry.
* **Achieved rate**: UDP bytes or packet count divided by nominal segment duration,
  capped by the remaining outer budget/next observed boundary, exactly as in the
  exploratory study. The denominator is saved as `budget_ms`; it is not the
  first-to-last span, wall time including timestamp draining, or a corrected
  packet-budget calculation. An observed empty phase records its real available
  `budget_ms` and zero achieved rate; a missing phase remains `observed=false`
  with zero fields. A logged phase already beyond its outer deadline can have
  zero remaining budget but must contain no packets. Compare packet counts as
  well as Mbps.
* **Boundary gap**: first TX in the phase minus the preceding packet's TX. D2
  naturally includes the 12 ms cooldown. Empty phases have a null gap, not zero.
* **Jitter**: median, nearest-rank p99, and population standard deviation of all
  adjacent kernel TX gaps. D2 includes intentional idle gaps, so use continuous
  controls for natural scheduler/batching jitter. Continuous controls also
  sample gaps crossing virtual boundaries every 20 ms from 100 to 1100 ms;
  the analyzer reports per-run baseline-gap median/p99/max/stddev distributions.
* **Pulse budgets**: D2 reports observed pulse count, total expected/actual pulse
  packets, min/max per-pulse packets, packet-budget ratio, and maximum drift.
  Final-phase packet/span/rate/gap metrics are separate from pulse totals.

`ns/op` includes socket setup and timestamp draining; use custom metrics and
artifacts rather than interpreting it as sender throughput. Go benchmark custom
metrics average across observed iterations; JSON analysis retains each iteration
separately. There are no pass/fail timing
thresholds, and loopback results do not establish NIC/on-wire performance.

## Fast validation (no pacing workloads)

```sh
go test ./pkg -run '^TestPacing(Observer|GapStats|PhaseBudget|ValidateTX|Metadata|Save)$' -count=1
python3 -m unittest discover -s benchmarks -p 'test_*.py' -v
```
