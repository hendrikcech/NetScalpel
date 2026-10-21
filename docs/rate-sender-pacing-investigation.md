# RateSender segment-boundary timing (#26)

## Scope

The original investigation below measured the static UDP `RateSender` at
commit `8c0e2fc`. It established the baseline before the deadline fix described
later in this document. No supplied segments are merged.

## Original investigation harness

The original opt-in `TestRateSenderPacingStudy` has been promoted to
`BenchmarkRateSender`, with automated artifact validation and comparison.
See [benchmarks/README.md](../benchmarks/README.md) for the current invocation,
source-overlay baseline procedure, case definitions, and artifact schema.
Run without `-race` and without competing benchmarks/tests. Missing kernel
timestamps fail the measurement instead of allowing application fallback.

The original harness called the unchanged `RateSender.Run` over IPv4 loopback,
drained the receiver, and observed a synchronous existing debug log just before
the ticker was created in each active segment. Thus phase assignment uses actual sequence
boundaries rather than nominal time windows. Zero-rate segments are preserved.
The observer adds one in-memory record per active segment, not per packet.
The sender's normal TX timestamp reader is enabled throughout.

Rates in the harness use 1408-byte UDP data (1400 bytes padding plus the 8-byte
sequence number): 44,389 pps for 500 Mbps and 8,877 pps for 100 Mbps. This excludes
UDP/IP/link headers. The CLI divides by padding size instead, making its nominal
500 Mbps approximately 502.86 Mbps of UDP data. That separate accounting
difference does not explain segment-boundary timing effects.

Original cases rotated order between repetitions (the persisted Go benchmark
runs repetitions grouped by case and records its invocation):

- One 1200 ms / 500 Mbps segment and one 1200 ms / 100 Mbps segment
  (continuous controls).
- Two 600 ms / 500 Mbps segments (equal-rate restart).
- 600 ms / 100 Mbps followed by 600 ms / 500 Mbps, and the reverse.
- D2: 50 repetitions of [8 ms / 500 Mbps, 12 ms / zero], then 600 ms / 500 Mbps.
  Both the normal 1600 ms outer timeout and a generous outer timeout are tested.
- One 1200 ms / 7 pps segment versus two 600 ms / 7 pps segments.
- Twelve 100 ms / 7 pps segments (sub-packet segment accounting).

The continuous control also samples packet gaps crossing virtual boundaries
every 20 ms between 100 and 1100 ms. These are time-boundary-crossing gaps, not
arbitrary adjacent-packet gaps: comparing restart gaps with the latter would
exaggerate the effect because most within-batch gaps are tiny.

## Original measurements

Measured on Linux 6.18.55, x86-64, four available CPUs, Go 1.26.8. Six
repetitions per case, without the race detector. All runs used complete kernel
software TX timestamps. These are local host measurements, **not NIC hardware
timestamps or proof of a corresponding physical-link artifact**. Receiver loss
and physical queue draining are not evaluated here.

### D2: cumulative drift is material

| Metric | Median | Range over six runs |
| --- | ---: | ---: |
| Probe start lateness, normal 1600 ms outer limit | 78.41 ms | 56.16–90.23 ms |
| Lateness attributable to transitions, excluding initial sender setup | 74.09 ms | 52.10–85.41 ms |
| Final probe first-to-last TX span, normal limit | 520.50 ms | 507.89–543.84 ms |
| Probe start lateness, generous outer limit | 76.58 ms | 42.23–97.89 ms |
| Final probe first-to-last TX span, generous limit | 598.58 ms | 598.25–598.79 ms |

The generous-limit control demonstrates that the late probe normally keeps its
full duration if the outer timeout does not cut it off. With the normal outer
limit, approximately 9–15% of the intended 600 ms probe is lost. Active probe
rates remain about 499–500 Mbps; the main problem is **when** the probe runs and
how much time remains, not inability to reach its target rate.

The 8 ms preconditioning pulses are also distorted. Their ideal budget is
355.112 packets. Median actual counts were 340 (normal outer limit) and 345
(generous limit), but ranges were 171–806 and 176–852 respectively. In 58 of
600 measured pulses the packet count exceeded 355. This is not just fractional
rounding: some active segments overran while timer cancellation was delayed,
and the cumulative-count pacer caught up inside those overlong segments.
One 806-packet pulse spanned 23.30 ms from first to last TX instead of fitting
inside 8 ms. Thus **both underfill and occasional overlong/overfilled pulses**
occur in this host environment. Each following zero-rate segment then starts
only after the preceding active segment actually exits.

Anchoring boundaries fixes accumulating phase drift but does not create a
hard-real-time sender. `ctx.Err()` is not a direct clock/deadline comparison:
the cancellation callback itself can be scheduled late. Explicit deadline
checks and a deliberate catch-up policy are relevant if suppressing late
traffic is required. Neither can retract packets already handed to the kernel.

### Nonzero-rate changes and equal-rate restarts

| Schedule | Actual gap across boundary, median | Range |
| --- | ---: | ---: |
| 500 → 500 Mbps | 1.245 ms | 1.171–2.286 ms |
| 100 → 500 Mbps | 2.031 ms | 0.304–2.718 ms |
| 500 → 100 Mbps | 1.227 ms | 1.156–2.332 ms |

In the continuous 500 Mbps control, 306 virtual-boundary gaps had median
0.072 ms, p95 1.186 ms, p99 1.258 ms and maximum 1.844 ms. Therefore an equal-rate
restart **systematically places a roughly tail-sized ordinary pacing gap at the
boundary**, but the measured magnitude is not a wholly new timescale compared
with existing host jitter. Six restarts are insufficient to characterize rare
tail events. The up/down change numbers should not be interpreted as isolating
rate direction from the differing natural packet intervals and scheduler noise.
For comparison, the continuous 100 Mbps control's 306 crossing gaps had median
1.297 ms, p95 1.459 ms, p99 1.512 ms and maximum 2.991 ms. Rate changes showed
millisecond-scale boundary gaps, but no separately established large transient
beyond the scale of those controls.

Over the nominal 1200 ms window:

| 500 Mbps schedule | Median packets | Packet range | Median achieved UDP-data rate |
| --- | ---: | ---: | ---: |
| One segment | 53,198 | 52,920–53,225 | 499.35 Mbps |
| Two equal-rate segments | 53,075 | 52,995–53,225 | 498.20 Mbps |

The difference between the medians is 123 packets, about 0.23%. The ranges
overlap, and the continuous control itself varies by 305 packets. This does
**not establish a large or statistically isolated throughput regression** from
a single equal-rate restart. It does establish a boundary-aligned gap. Most
of the apparent second-segment start lateness in these two-segment runs is
initial sender setup under the outer timer; incremental transition lateness
for equal-rate runs was only 0.050–0.368 ms (median 0.136 ms).

### Low rates: the reset is visible even without a count difference

Both one 1200 ms / 7 pps segment and two 600 ms / 7 pps segments transmitted
eight packets in all six runs. However, the split schedule's boundary gap was
170.47–173.22 ms (median 172.27 ms), versus a nominal continuous packet spacing
of 142.86 ms. The first segment reaches four packets after roughly 571.43 ms;
its remaining 0.2 packet credit is discarded, so the next packet requires a
fresh 142.86 ms after the 600 ms boundary. Identical total counts conceal this
phase discontinuity.

Twelve 100 ms / 7 pps segments transmitted **zero packets in all six runs**,
versus eight in the continuous 1200 ms control. This is the clearest example of
segment-local packet accounting changing actual traffic despite equal total
duration and equal requested rates. Preserving segment structure does not
require discarding fractional credit; carrying it would be an explicit policy
change, separate from deadline anchoring.

### Ordinary jitter and catch-up are already substantial

Across the six continuous 500 Mbps runs, each run's largest adjacent-packet gap
ranged from 1.74 to 8.10 ms, even though there were no segment transitions.
Peak counts in fixed 10 ms bins ranged from 510 to 915 packets (nominal demand
443.89 packets/10 ms). The two-segment equal-rate runs had peaks of 492–795.
These observations support treating restart discontinuity and general
catch-up burstiness as separate effects, rather than attributing every burst
or gap near a boundary to switching rates.

Local raw artifacts: `/tmp/opencode/pacing-study/` (initial eight cases),
`/tmp/opencode/pacing-short/` (short 7 pps segments), and
`/tmp/opencode/pacing-100/` (100 Mbps control). Each contains `summary.json` and
per-run packet CSVs. The last two controls were run separately, after the
initial study, without concurrent measurement workloads.

## Original code-level findings

1. **Phase boundaries drift cumulatively.** `pkg/udp.go:451` creates each
   `context.WithTimeout` only when the previous segment has actually returned.
   Timer delivery lateness and transition work shift every following phase.
   The outer duration is still the sum of nominal durations
   (`pkg/client.go:153`, `pkg/server.go:403`), so later phases lose time rather
   than extending the overall test. The zero-rate path waits on the same child
   context and therefore also contributes timer wake-up lateness.
2. **Every active segment restarts pacing.** `pkg/udp.go:510` establishes a new
   start time, ticker and `numPacketsSent`, even across equal rates. There is no
   immediate first send and no final flush to the segment's exact packet goal.
   Unserved demand from the previous segment is discarded. The next segment
   does not inherit a high-rate debt when switching to a lower rate.
3. **Fractional packet credit is discarded at each boundary.** With ideal
   timing, `floor(rate1 * duration1) + floor(rate2 * duration2)` can be smaller
   than rounding an accumulated fractional budget. In particular, a 100 ms
   segment at 7 pps cannot reach a one-packet goal before its deadline. Repeated
   short segments can therefore transmit nothing despite a nonzero requested
   average rate. Actual end-of-segment underfill can exceed the fractional
   effect because the final ticker wakeup need not occur at the deadline.
4. **General catch-up bursts are not specific to switching.** The ticker asks
   for about five packets per tick, but `packetsDue` computes all outstanding
   demand and `sendRound` can send it in batches up to 1024. Scheduler lateness
   can therefore produce much larger bursts. Ticker channel timestamps are
   scheduled times, not necessarily the time the goroutine executes a send.
5. **A blocked send can outlive a child segment deadline.** Only the outer
   context sets a socket write deadline. Child cancellation is checked between
   batches, but cannot interrupt an already-blocked `SendMsgs`. This is a code
   risk, not a demonstrated loopback result.

## Fix direction from the investigation

Anchor segment deadlines to one common sender start plus cumulative nominal
durations. Preserve all segment boundaries. Skip segments whose end is already
past, including zero-rate segments; do not replay expired traffic or extend the
overall schedule. Define whether a partially missed segment should drop missed
demand or catch it up, rather than accidentally introducing a boundary burst.
Preserve cancellation and decide separately how blocked sends observe segment
deadlines.

Anchored deadlines alone do not preserve fractional packet credit, avoid ticker
restart gaps, or eliminate general scheduler/catch-up jitter. Those are separate
pacing-policy decisions; coalescing equal-rate segments is not required or
authorized by this investigation.

## Original investigation validation

All 60 measurement runs passed with complete kernel TX timestamps. Production
sender code was not changed. `go test ./pkg -skip BBR` passed. The unfiltered
package suite failed only `TestTCPDLBBR`, because this kernel does not provide
the requested `bbr` congestion-control algorithm (`no such file or directory`).
The measurement harness is observational and opt-in; it is not regression
coverage for a deadline fix that has not yet been implemented.

## Implemented deadline policy

`RateSender.Run` now takes one monotonic-clock start before sender setup. Every
positive-duration segment ends at that start plus the cumulative nominal
durations. Nonpositive-duration entries remain in the supplied parameter list
but consume no schedule time or packets. No equal-rate entries are coalesced.
The outer context remains authoritative if it expires sooner.

Fully expired segments are skipped, including zero-rate cooldowns. A partially
missed segment only runs for its remaining window: whole-packet demand from
before entry is subtracted, rather than replayed as a boundary catch-up burst.
While the segment is live, the existing cumulative catch-up policy remains in
force. Packet goals are capped to the segment budget. Fractional credit and
unsent debt still do not cross segment boundaries, and the ticker is still
restarted; this change does not silently redefine those pacing policies.

The send loop checks the actual monotonic deadline as well as `ctx.Err()`;
timer-callback delivery lateness no longer authorizes sending expired demand.
Each segment installs an effective socket write deadline (the earlier of its
own end and the outer deadline), so a blocked send cannot borrow the next
segment's window. Cancellation callbacks are joined before clearing the socket
deadline, preventing a stale callback from poisoning a later segment. There
are checks before packet preparation and immediately before `SendMsgs`, but
software cannot retract a syscall already in progress or packets queued below
the socket. This is not a hard-real-time or hardware pacing guarantee.

Controlled-clock tests cover cumulative boundaries, equal-rate segment
preservation, fully and partially missed segments, cooldowns, nonpositive
durations, delayed context cancellation, blocked sends, socket deadline resets,
and outer cancellation. The packet-accounting and ticker-reset artifacts at
low rates intentionally remain measurable in the benchmark.

## Same-harness before/after benchmark

The persisted `BenchmarkRateSender` was run against the original `8c0e2fc`
`pkg/udp.go` via a Go source overlay, then against the fixed implementation.
Both used the **identical benchmark source**, dependencies, Linux 6.18.55,
Go 1.26.8, and GOMAXPROCS=4. Each version ran six repetitions of all 11 cases
(132 measured workloads total); no test, build, or other performance workload
ran concurrently. The default strict analyzer accepted every raw CSV and JSON
sidecar, complete kernel timestamps, unique repetitions, derived measurements,
and compatible environments without an override.

Full distributions, source hashes, and provenance are persisted in
[`issue26-benchmark-results.json`](issue26-benchmark-results.json). The original
and fixed production source SHA-256 values are respectively
`43fc0ef35784b9f0cf79c90640bf26c60b30b0b05a73bb1ee027e6a637948614` and
`47e44b1fdc0b678fe4daa78e602a9cde8b319d9bc1e1449cc70c243d7796a593`.
Git revision alone identifies neither overlaid nor dirty code; use these hashes.
Raw local artifacts are in `/tmp/opencode/issue26-before/` and
`/tmp/opencode/issue26-after/`. The baseline ran first, fixed second; these are
unpaired distributions, not a statistical significance test.

### D2 timing and packet budgets

Values are median [minimum, maximum] across six runs unless stated otherwise.

| Metric | Before | Fixed |
| --- | ---: | ---: |
| Final probe start lateness, normal outer limit | 91.41 [64.77, 111.65] ms | 0.784 [0.731, 1.203] ms |
| Final probe first-to-last TX span, normal limit | 507.08 [486.64, 534.27] ms | 597.75 [596.88, 597.86] ms |
| Final probe start lateness, generous outer limit | 84.57 [74.42, 95.09] ms | 0.858 [0.722, 0.922] ms |
| Final probe first-to-last TX span, generous limit | 598.71 [598.50, 599.87] ms | 597.66 [596.63, 597.90] ms |
| Active final probe rate, normal limit | 499.81 Mbps | 499.81 Mbps |
| Total preconditioning pulse packets, normal limit | 16,866.5 [16,428, 17,670] | 14,648.5 [14,277, 15,073] |
| Total preconditioning pulse packets, generous limit | 16,925.5 [15,745, 17,538] | 14,781 [14,070, 15,506] |
| Largest pulse packet count across both variants/all runs | 886 | 344 |

All 50 pulses and the final active phase were observed in every run. No fixed
pulse exceeded its 355-packet floor budget. Median final-probe lateness with
the normal outer limit fell by approximately **99.1%**, recovering about
**90.7 ms** of actual packet span. A generous caller timeout no longer extends
the sender's nominal schedule: the sender still finishes its phases at its own
cumulative deadline.

**The reduced preconditioning packet count is a real tradeoff, not a throughput
improvement.** Stricter cutoffs stop late traffic instead of borrowing time from
cooldowns. Dropping demand missed before late segment entry and declining to
flush a final batch after the deadline reduce pulse totals. Against the nominal
17,750-packet floor budget, median fixed totals are 82.5% (normal limit) and
83.3% (generous limit), versus 95.0% and 95.4% before. Previously overlong pulses
could mask underfilled pulses. Exact short-pulse packet counts would require a
separate pacing policy; this fix prioritizes nominal phase timing and no replay.

### Boundary gaps are not eliminated

| Transition | Before median [range] | Fixed median [range] |
| --- | ---: | ---: |
| 500 → 500 Mbps | 1.233 [1.184, 1.244] ms | 2.306 [1.152, 2.379] ms |
| 100 → 100 Mbps | 1.902 [1.346, 2.818] ms | 2.663 [1.302, 2.963] ms |
| 100 → 500 Mbps | 1.356 [0.262, 2.702] ms | 2.730 [1.499, 2.965] ms |
| 500 → 100 Mbps | 1.219 [1.178, 2.241] ms | 1.274 [1.241, 2.282] ms |

Equal-rate and upward-transition median gaps were **larger**, not improved, in
this collection. Strict end checks remove sends permitted by delayed
cancellation in the old sender; the next segment still waits for a new ticker.
That combination can expose the final unsent tail plus restart latency. Six
repetitions and fixed before/after order cannot isolate every scheduler effect,
but these results are not evidence of pacing continuity. Preserving the segment
list does not prohibit a future phase-continuous pacer; no such policy change
or fractional-credit carry is smuggled into this deadline fix.

Over the nominal 1200 ms window, median packet counts and UDP-data rates were:

| Schedule | Before packets / rate | Fixed packets / rate |
| --- | ---: | ---: |
| One 500 Mbps segment | 53,172.5 / 499.11 Mbps | 53,205 / 499.42 Mbps |
| Two 500 Mbps segments | 53,155 / 498.95 Mbps | 53,108 / 498.51 Mbps |
| One 100 Mbps segment | 10,632.5 / 99.80 Mbps | 10,633.5 / 99.81 Mbps |
| Two 100 Mbps segments | 10,620 / 99.69 Mbps | 10,622 / 99.71 Mbps |

Fixed equal-500 median packet count was 0.088% below the baseline; that is a
small aggregate effect, despite the visible boundary gap. Continuous 500 Mbps
controls' per-run virtual-boundary p99 gaps had medians 1.737 ms before and
1.253 ms fixed (ranges 1.192–3.827 and 1.230–1.271 ms). Natural host jitter and
batching remain important, and software loopback timestamps still do not prove
physical-link behavior.

### Low-rate accounting remains intentionally unchanged

One 1200 ms / 7 pps segment and two 600 ms / 7 pps segments still sent eight
packets in every run, before and after. Split-phase median boundary gaps were
171.22 ms before and 171.65 ms fixed. Twelve 100 ms / 7 pps segments still sent
zero packets. Anchored deadlines improve the final short phase's median start
lateness from 6.342 ms to 0.258 ms, but do not carry its fractional credit.

### Final validation and review

- Independent subagents implemented the sender and benchmark; another reviewed
  both, with follow-up review of the strengthened tests and artifact validation.
  No blocking findings remain.
- `go test -race ./... -skip BBR` passed after all implementation changes.
- `go vet ./...` and `go build ./...` passed.
- All 18 Python analyzer tests passed; CI runs them without performance workloads.
- All 132 benchmark workloads passed complete-kernel timestamp collection and
  strict artifact analysis. The same benchmark-source hash was used on both sides.
- BBR integration tests are excluded locally because the host only provides
  Reno/Cubic. No claim is made about BBR or a privileged network-emulation run.
