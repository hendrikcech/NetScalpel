# Static UDP rate stability benchmark

This is an **informational benchmark**, not a timing-sensitive CI assertion.
It runs the real `scalpel-run udp-rate` sender unchanged, captures its outgoing
packets independently with tcpdump, and writes a plain-text report. Analysis
runs **after** each transfer, so Python analysis does not compete with pacing.

## Namespace E2E topology

```sh
nix run ./e2e#udp-stability
```

Reuses the existing client/router/server E2E topology, prefilled ARP, and common
host clock, **without netem delay or a rate-limited bottleneck**. Captures are
on the client's `veth0`, before the router. The sweep is 10, 100, 250, 500,
750, and 1000 nominal Mbps, three 2-second transfers per rate. It is deliberately
not part of `e2e#all`: whether a host maintains a particular rate is not portable.

Output: `out/e2e-udp-stability/<run>/client/stability/report.txt`, per-transfer
`.txt`, `.pcap`, `.csv`, sender/capture logs, `summary.json`, and `metadata.json`.

## Standalone / actual sender host

Requirements: Linux, Python >=3.11, a normal (not race-instrumented) build of
`scalpel-run`, tcpdump with nanosecond pcap support, and packet-capture permission
(`CAP_NET_RAW` or root in the relevant network namespace).

For a privilege-isolated loopback baseline, with `tcpdump` on PATH:

```sh
go build -o /tmp/opencode/scalpel-run ./cmd/scalpel-run
unshare -Urn sh -c 'ip link set lo up &&
  python3 e2e/udp_stability.py run --binary /tmp/opencode/scalpel-run \
    --start-server --out out/stability-loopback'
```

If user namespaces are disabled, use an allowed network namespace or explicitly
run with appropriate capture privileges. A private loopback isolates other
traffic; it does **not** isolate the machine's CPU scheduling.

To test the original slower sender, start `scalpel-run server` on the receiver,
then run on the **sender**, with an interface and server IPv4 address:

```sh
python3 e2e/udp_stability.py run --binary ./scalpel-run \
  --interface eth0 --ip 192.0.2.2 --rates 100 250 500 750 \
  --duration-ms 5000 --repeats 5 --out out/stability-real-host
```

Use an unshaped path with enough capacity. This runner tests **UL only**: for
DL, run it on the other endpoint with roles swapped. Do not capture `any`
across a router: multiple observations of the same packet invalidate the run.
Avoid other UDP traffic to the target; unrelated matching UDP packets invalidate
the run rather than being silently discarded. Existing output directories are
never overwritten. Long runs retain all records in memory during analysis.

### Controlled CPU experiments

Add to a `run` command (use a new `--out` directory each time):

```sh
--sender-cpu 1                           # constrain all sender threads to one CPU
--sender-cpu 1 --compete                 # add one busy process on that CPU
--sender-cpu 1 --gomaxprocs 1             # constrain Go parallelism as well
--pause-ms 60 --pause-at-ms 100           # deliberate whole-sender SIGSTOP/CONT
```

The pause is injected about 100 ms after the first buffered capture records
become visible; this is not an exact packet-time trigger. Its actual elapsed
duration and offset from captured seq=0 are retained in metadata; a pause
outside the transmission interval is rejected. It tests the **response to a known external
stall**, not whether the original unexplained dip was caused by that stall.
CPU pinning/contention are not equivalent to reducing CPU frequency or imposing
a cgroup quota. Examine the original host's `cpu.max`, `cpu.stat`, GC/runtime
traces, and hypervisor contention before assigning a cause to a 60 ms dip.

Run profiles **sequentially**, with no builds/tests/other benchmarks alongside.
For capture overhead controls, run the same CLI directly without tcpdump and
compare its CSV kernel-TX window distribution; these controls are not independent
ground truth. `GODEBUG=gctrace=1` can be passed through the environment; stderr
is retained in each transfer's `.stdout` file. That tracing also adds overhead.

## Report interpretation

- **Rate accounting:** the CLI floors pps using 1400 bytes, but currently sends
  **1408-byte UDP payloads** (8-byte sequence plus 1400 padding). Reports show
  both the requested rate and the resulting effective payload goal. At nominal
  500 Mbps the latter is **502.847488 Mbps**. Neither is Ethernet wire rate;
  IP/UDP/Ethernet overhead, preamble, FCS and inter-frame gap are not included.
- Fixed, non-overlapping **1/5/10/20/60 ms windows**, anchored to captured seq=0.
  Empty windows count as zero; incomplete final windows are excluded. Reported
  rates are bytes inside each window divided by its full width, **not**
  packet count divided by first-to-last arrival span.
- Each scale reports mean, min/p01/median/p99/max, coefficient of variation,
  low/high outlier counts and percentage, and longest consecutive low/high run.
  Default outlier: outside **+/-10% of effective goal**; change `--tolerance`.
  Window phase and packet quantization affect outlier percentages, particularly
  at low rates and 1 ms resolution. These are operational thresholds, not a
  statistical confidence test or an intrinsic property of the sender.
- Startup (first 60 ms and first twelve 10 ms bins) is retained. A separate
  >=100 ms distribution avoids mixing startup with steady state. Origin at
seq=0 cannot see silence before the first packet; do not interpret it as the
RPC scheduled-start instant. When sender logs are supplied, first-packet delay
from the run/pacer startup log is also shown (log precision is only ~1 ms).
The final configured interval can include normal
  termination shortfall, which is reported rather than trimmed away.
- Cumulative traffic error distinguishes temporary deficit/catch-up from a
  persistent capacity shortfall. Interpacket gaps expose stalls hidden by means.
- Every captured sequence/size is matched to CSV, with missing/duplicate/extra
  packets and tcpdump kernel drop counts checked. Any mismatch or missing drop
  statistics makes the capture **INVALID** (exit 1); rate outliers alone do not.
  Capture and CSV TX times are compared per sequence and in common 10 ms bins.
  Nanoseconds are preserved; timestamps are never rounded through Python floats.
- Sender logs identify whole-run userspace TX timestamp fallback. A capture can
  remain valid when CSV timestamps fall back, but the CSV then measures a
  different event. Retained logs also expose socket-buffer and partial-send warnings.

**Observation point:** tcpdump timestamps are kernel packet-tap timestamps,
not when tcpdump wakes or flushes its file. Delayed file writes do not themselves
create a timestamped gap. Both capture and CSV software TX still share the
kernel clock/network path: agreement rules out a CSV-only artifact, **not**
NIC/qdisc buffering or hardware-wire burstiness. For that, capture at a separate
receiver/tap or use suitable hardware timestamps. Loopback/veth results do not
establish physical-NIC performance. Capturing itself consumes CPU and can alter
the workload; zero capture drops does not prove zero observer effect.

Reanalyze retained artifacts without transmitting:

```sh
python3 e2e/udp_stability.py analyze --rate 500 --duration-ms 2000 \
  --pcap out/stability-loopback/rate-500-run-1.pcap \
  --csv out/stability-loopback/rate-500-run-1.csv \
  --capture-log out/stability-loopback/rate-500-run-1.capture.log \
  --sender-log out/stability-loopback/rate-500-run-1.sender.log
python3 -m unittest discover -s e2e -p 'test_udp_stability.py' -v
```
