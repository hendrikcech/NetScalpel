# End-to-end correctness smoke tests

Runs `scalpel-run` client and server across an emulated link
([nixnet](https://github.com/birneee/nixnet) network namespaces + netem) and
checks that measurements respond correctly to configured impairments. These
short tests produce a pass/fail verdict, with raw diagnostics retained. Tight
performance characterization is separate: see [benchmarks](../benchmarks/README.md).
The execution backend (nixnet versus loopback) does not determine whether a run
is a smoke test or a benchmark.

## Topology

```
client (10.0.0.1) ── router (10.0.0.2 / 10.0.1.1) ── server (10.0.1.2)
```

netem is applied on the *router's* egress interfaces only — never on the
sender's own interface, where the delay would happen before the kernel takes
the software TX timestamp and thus be invisible to the measurement.

## Scenarios

| Package | Emulation | Assertions |
|---------|-----------|------------|
| `owd` | 20 ms delay each direction | UL/DL median OWD near configured delay, bounded late-outlier fraction and catastrophic ceiling, strict delay floor, valid CSV/timestamp arithmetic, 0 lost, positively verified kernel TX timestamps |
| `loss` | 5 ms delay + 5% loss each direction | observed loss over 2500 packets per direction within 3 percentage points of 5%; strict profile uses a 4σ binomial interval; lost rows have empty `ts_rcvd`/`owd_ms` |
| `rate` | 10 ms delay, 50 Mbit rate, 200-packet queue | `udp-rate` at 80 Mbps: valid received data and broad rate/delay sanity; strict profile additionally checks ±10% wire rate and queue growth/tails |
| `asym` | 30 ms UL / 10 ms DL delay | UL and DL runs show their direction's delay (catches swapped-direction bugs) |
| `icmp-udp` | 20 ms delay each direction | `icmp` and `udp-periodic` each within OWD bounds and their medians agree within 1 ms |
| `tcp` | 20 ms delay, 50 Mbit rate each direction | valid TCP samples and broad RTT/throughput sanity; strict profile additionally checks tight RTT and ≥50% bottleneck utilization |
| `duplicates` | DL: 20 ms delay + 100% duplication | Every sent sequence captured twice; CSV records both exact receive timestamps, `dup_count=1`, earliest-arrival OWD, one row per sent packet |
| `reordering` | DL: 50 ms delay, every fifth packet bypasses delay | Capture must show first-arrival sequence inversions; CSV preserves each exact arrival timestamp and earliest-arrival OWD, no loss or extra rows |

## Usage

```sh
nix run ./e2e#owd    # from the repo root
nix run ./e2e#loss
nix run ./e2e#duplicates
nix run ./e2e#reordering
nix run ./e2e#owd 1-5   # repeat 5 times (indexed output dirs)
nix run ./e2e#all       # run every scenario back to back; exits non-zero if any fails
nix run ./e2e#smoke     # explicit alias for the correctness suite
```

The exit code is the test verdict. Root is not required (nixnet uses user
namespaces).

Results land in `out/<scenario>/<run>/`: each node's working directory
(`client/`, `server/`) contains the CSVs and debug logs it wrote, so failed
runs can be inspected directly. `verify.py` holds the assertions; it is
invoked by the experiment's `postRun` hook but can also be run by hand against
an existing output directory:

```sh
python3 e2e/verify.py owd --csv out/e2e-owd/00/client/owd_ul.csv --delay-ms 20
python3 e2e/verify.py owd --profile strict \
  --csv out/e2e-owd/00/client/owd_ul.csv --delay-ms 20
```

### OWD policy and shared analysis

Both endpoints share the host clock, but a configured netem delay is not an
exact per-packet upper bound: qdisc release and host scheduling can add delay.
Smoke checks therefore tolerate a small, explicitly bounded fraction of late
packets rather than failing on one slightly late packet. They do not discard
outliers, relax the delay floor, or waive structural errors. The `strict` profile
retains every-packet bounds for deliberate validation on controlled hosts.
Smoke thresholds are explicit policy defaults, not a claim about CI tail latency;
use benchmark distributions to tune them if runner characteristics change.

`measurements/udp.py` owns the common nanosecond CSV parser, consistency checks,
and OWD summary used by both smoke assertions and offline benchmark reporting.
`scenario.nix` owns nixnet topology/lifecycle, including bounded TCP readiness
instead of a fixed startup sleep. Router-only impairment placement and ARP
prefill are preserved. CI uploads diagnostics even when a smoke test fails.

### OWD accuracy benchmark (no latency pass/fail budget)

```sh
nix run ./e2e#benchmark-owd 1-3
```

This reuses the exact OWD workload and topology, but evaluates measurement
validity rather than tight delay acceptance. Reports land in
`out/benchmark-owd/<run>/report/`: JSON, CSV, Markdown, and SVG. Slow packets
remain visible in distributions; missing/corrupt data or timestamp fallback
still fails. Report generation also works offline; see the benchmark README.
The CI benchmark workflow publishes reports in job summaries and retains raw
data plus generated artifacts. Hosted-runner results are descriptive, not
dedicated-host regression baselines.
The benchmark wrapper records individual repetition exit statuses in
`out/benchmark-owd/execution.json` and fails if any run fails. The older smoke
scenario range commands use nixnet's native range loop, which suppresses
individual failures; use single-run invocations for gating (as CI does).

### Duplicate and reordering ground truth

These two scenarios use `udp_delivery.py` to capture **inbound packets at the
client receiver**, after the router's qdisc. The independent capture is compared
against every CSV sequence, payload size, copy count, and receive timestamp at
nanosecond precision. OWD must use the earliest captured arrival. Identical
timestamps still represent separate copies. Missing capture-drop statistics or
nonzero kernel capture drops fail the test; pcaps and logs remain in the run
directory. These scenarios currently cover downlink UDP, not uplink or ICMP.

Netem percentage-based impairments are normally probabilistic, and a seed does
not eliminate scheduling variation. Here `duplicate 100%` avoids random selection;
`delay 50ms reorder 100% gap 5` lets every fifth packet bypass the delay. We do not
assume a fixed reordered sequence: the capture must demonstrate actual reordering
and the CSV must match what arrived. A run with no reordering fails rather than
passing vacuously. The duplicate scenario requires exactly two copies of every
sent packet. Nixnet's pinned interface exposes neither option, so these qdiscs
are installed with `tc` in the router's post-setup hook.
Only the eight-byte measurement datagrams are classified into netem; empty
NAT-probe replies and TCP control traffic bypass it. This keeps reordered data
from racing the probe handshake, which otherwise consumes the first UDP reply.

Run the oracle's regression tests without network privileges:

```sh
python3 -m unittest discover -s e2e -p 'test_udp_delivery.py'
```
