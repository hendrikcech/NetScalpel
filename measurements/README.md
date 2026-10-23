# Measurement infrastructure

Classify a run by its **purpose**, independently of its execution backend:

* **Smoke/correctness:** short, required pass/fail checks. Strict structural
  invariants and broad response checks; retain diagnostics on failure.
* **Benchmark:** characterize distributions, accuracy, stability, and rate
  behavior. Always retain raw data; poor performance is a result, not an
  execution failure. Invalid or incomplete measurement data still fails.

Supported entry points:

```sh
nix run ./e2e#smoke
nix run ./e2e#benchmark-owd 1-3
python3 benchmarks/run.py pacing --out out/pacing-study --repetitions 3
```

Nixnet currently provides controlled-link smoke tests and UDP OWD accuracy;
loopback provides RateSender pacing/stability. The common UDP analyzer also
accepts CSVs collected elsewhere; this does not imply a remote orchestration
backend exists. Go microbenchmarks remain normal `go test -bench` benchmarks.

## Boundaries

1. `e2e/scenario.nix`: nixnet topology, impairment placement, readiness,
   endpoint lifecycle, and collection directories.
2. `measurements/udp.py`: backend-independent UDP CSV parsing, nanosecond
   timestamps, structural invariants, and descriptive OWD metrics.
3. `e2e/verify.py`: named pass/fail policies over measurements.
4. `benchmarks/analyze.py`: strict validation of the existing Go pacing
   benchmark's raw packet data, schedule accounting, and provenance.
5. `benchmarks/run.py` and `benchmarks/report.py`: benchmark invocation and
   offline, versioned reports. Reports do not rerun traffic.

Keep **execution**, **measurement validity**, and **acceptance** separate.
An execution manifest saying `complete` does not mean a performance target was
met. Missing provenance is unknown, not proof of a particular timestamp path or
environment. A report may be useful even when execution failed; validity errors
must remain visible, and incomplete populations must not masquerade as valid
repetitions. Acceptance defaults to `not_evaluated` for benchmarks.

The experiment CLI's `info.json` remains its execution record; benchmark reports
are analysis products, not a replacement or a reinterpretation of that schema.
See [benchmark methodology and artifacts](../benchmarks/README.md) and
[smoke policies](../e2e/README.md).

## CI

`build.yml` gates correctness and uploads smoke diagnostics. `benchmarks.yml`
runs on relevant PRs, weekly, and manually; it publishes Markdown job summaries
and uploads raw inputs plus JSON/CSV/SVG reports. Benchmark jobs fail on broken
execution or invalid measurements, **not** on an arbitrary jitter/throughput
budget. No privileged PR-commenting workflow runs untrusted branch code.

Hosted runners provide descriptive observations, not controlled performance
baselines. Compare revisions on matching, idle environments before establishing
regression thresholds. Never add benchmark retries that hide bad observations.

Fast Python validation, without network privileges:

```sh
python3 -m unittest discover -s measurements -p 'test_*.py' -v
python3 -m unittest discover -s e2e -p 'test_*.py' -v
python3 -m unittest discover -s benchmarks -p 'test_*.py' -v
```
