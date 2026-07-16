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

## Usage

```sh
nix run ./e2e#owd    # from the repo root
nix run ./e2e#loss
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
