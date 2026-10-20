# End-to-end measurement validation

Runs `scalpel-run` client and server across an emulated link
([nixnet](https://github.com/birneee/nixnet) network namespaces + netem) and
asserts that the values in the produced CSVs match the configured ground
truth. See `plans/END2END.md` for background.

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
| `owd` | 20 ms delay each direction | UL and DL `owd_ms` within `[19, 22]` ms, `ts_sent` strictly monotonic, 0 lost, kernel TX timestamps used (no userspace fallback in the logs) |
| `loss` | 5 ms delay + 5% loss each direction | observed loss over 2500 packets per direction within a 4σ binomial interval around 5%, lost rows have empty `ts_rcvd`/`owd_ms` |
| `rate` | 10 ms delay, 50 Mbit rate, 200-packet queue | `udp-rate` at 80 Mbps: receive rate = 50 Mbps ±10% (wire), first packet near the bare delay, p90 `owd_ms` beyond half the queue-full plateau, max `owd_ms` bounded by the queue limit |
| `asym` | 30 ms UL / 10 ms DL delay | UL and DL runs show their direction's delay (catches swapped-direction bugs) |
| `icmp-udp` | 20 ms delay each direction | `icmp` and `udp-periodic` each within OWD bounds and their medians agree within 1 ms |
| `tcp` | 20 ms delay, 50 Mbit rate each direction | `tcp` UL and DL: RTT ≈ 40 ms at start, `MinRTT` ≈ 40 ms, acked throughput ≤ link rate and ≥ half of it |
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
```

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

## Static UDP pacing benchmark

`nix run ./e2e#udp-stability` runs an unshaped send-rate sweep with independent
sender-side packet capture and a plain-text stability/outlier report. It is
informational and excluded from `#all`. See [UDP_STABILITY.md](UDP_STABILITY.md)
for standalone use on the original sender, CPU-contention/pause controls,
timestamp validation, and measurement limitations.
