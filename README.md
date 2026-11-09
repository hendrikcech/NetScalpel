<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="netscalpel-dark.svg">
    <img src="netscalpel.svg" alt="NetScalpel" width="450">
  </picture>
</p>

<p align="center">
  A composable measurement toolkit to surgically dissect link and queue dynamics.
</p>

## Overview

NetScalpel is a custom network measurement tool designed for fine-grained control over packet transmission timing and scheduling. Written in Go, it utilizes a client-server architecture to coordinate test execution and exchange results over a TCP control channel. UDP measurements can use kernel timestamps to compute one-way delay (OWD), packet loss, and receive rates; timestamp availability and accuracy depend on the selected source, hardware, and clock configuration. Accurate one-way delay analysis requires clock synchronization between the client and server endpoints, for example using NTP.

## Versions

- **v1.0** is the original released version used for the SIGCOMM paper below
  (commit `121a171`, tag `v1.0.0`).
- **v2.0 is currently in development.** It breaks output compatibility by replacing
  `scalpel-exp`'s `info.txt` with `info.json` and adding duplicate-reporting CSV
  columns (`dup_count` and additional receive timestamps).

<details>
<summary>Changes since v1.0</summary>

- Added JSON execution manifests, binary version reporting, and UDP duplicate
  packet timestamps in CSV output.
- Added configurable UDP timestamp sources and reports, including hardware RX
  timestamps, with explicit errors when the selected source is unavailable.
- Added batch experiments with nested groups, parameter products, and result
  summaries.
- Improved UDP send pacing, performance, and partial-write handling.

</details>

In v2, use `--version` with either binary to check its version.

## Citation

NetScalpel was developed and used for the measurements presented in the paper *Dissecting the StarLink: Characterizing Queuing and Flow Dynamics in the Starlink Network*, published at SIGCOMM'26. 

If you use this tool in your research, please cite our paper:

<details>
<summary>BibTeX</summary>

```bibtex
@inbook{cech2026starlink,
author = {Cech, Hendrik and Mohan, Nitinder and Ott, J{\"o}rg},
title = {Dissecting the StarLink: Characterizing Queuing and Flow Dynamics in the Starlink Network},
year = {2026},
isbn = {9798400724671},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3789240.3829162},
abstract = {Starlink has become the largest commercial LEO satellite network, yet little is known about its internal queue management and bandwidth allocation mechanisms. Prior measurement studies have documented performance variations but lack the granularity to explain the underlying causes. We present the first microscopic characterization of Starlink's transmission behavior, using controlled measurements from multiple terminals to capture per-packet dynamics at microsecond precision. Our analysis uncovers several previously undocumented mechanisms. Starlink employs head-drop queuing rather than tail-drop, with capacities of approximately 1500 and 4000 packets on downlink and uplink, respectively. Bandwidth allocation is demand-driven, starting from a baseline of 100/30 Mbps on the downlink and uplink that ramps up by 3.4{\texttimes}/2{\texttimes} over 400 ms when flows sustain queue pressure. Active queue management aggressively induces packet loss to control queue occupancy, especially on the uplink. These mechanisms reset every 15 seconds during Starlink's reconfiguration cycle. We also find flow-level queuing that isolates latency between concurrent flows while coupling their loss on the downlink. These findings reveal that Starlink's queue management creates fundamentally different operating conditions than terrestrial networks.},
booktitle = {Proceedings of the ACM SIGCOMM 2026 Conference},
pages = {1475–1495},
numpages = {21}
}
```

</details>

## Building

NetScalpel requires Go to be installed. It operates strictly on Linux systems as it relies on kernel-level packet timestamps.

To build the executable binaries, run the following commands from the root of the project:

```sh
go build ./cmd/scalpel-run
go build ./cmd/scalpel-exp
```

## Using `scalpel-run`

The `scalpel-run` binary is the primary tool for executing individual network measurements, such as UDP and TCP tests.

### Server Mode

Before running any tests, start the server component on the target machine:

```sh
./scalpel-run server
```

By default, the server listens for connections on `0.0.0.0:8500`.

### Client Mode

The client connects to the server to perform targeted network tests. Available commands include:
- `udp-burst`: Transmit a burst of UDP packets at line rate to investigate queue capacity and drop policies.
- `udp-rate`: Transmit UDP packets at a configurable send rate, duration, and packet size to probe link response to sustained load.
- `udp-periodic`: Send individual UDP packets at regular intervals to measure baseline OWD without inducing queuing.
- `tcp`: Execute TCP measurements with configurable parameters such as duration and bytes.
- `icmp`: Send ICMP echo requests at a constant interval.

**Example: Running a Periodic UDP Test**

Start the client to send UDP packets for 1 second with a gap of 200 ms:

```sh
./scalpel-run udp-periodic --ip 127.0.0.1 --interval=200 --duration=1000
```

*Note: Tests run in the uplink (UL, from client to server) direction by default. This can be changed using the `--direction dl` flag.*

## Using `scalpel-exp`

The `scalpel-exp` binary orchestrates experiments that schedule measurements
ahead of time. This prevents mixing measurement and control traffic during an
experiment.

The terms used by `scalpel-exp` have distinct meanings:

- An **experiment** is one complete `scalpel-exp client` run.
- A **procedure** is a named, reusable measurement definition. An experiment can
  invoke one or more procedures.
- A **round** is one repetition of that procedure within the experiment.
- A **procedure invocation** is one scheduled execution within a round. A
  per-direction procedure may have separate DL and UL invocations.

To view the list of supported procedures:

```sh
./scalpel-exp procedures          # all registered procedures
./scalpel-exp procedures prograte # details for one procedure
```

The available procedures include `MultiDurationRate`, `Burst`, `Cooldown`,
`SwitchFlow`, `Rate`, `OWD`, `MouseElephant`, and `TCPReconf`.

To run an orchestrated experiment, you typically start the `scalpel-exp server` on the remote endpoint and the `scalpel-exp client` on the local endpoint, passing the necessary flags for the desired procedure.

To run multiple procedures from a YAML file, use
`scalpel-exp client --batch batch.yaml --ip <server-ip>`.
Batch mode supports sequential blocks, shuffled
blocks, and randomly interleaved rounds, including nested groups. See the
[batch mode guide](docs/batch-mode.md) for examples and result summaries.

## Implementing New Procedures

Experiment procedures live in [`cmd/scalpel-exp/procedures`](cmd/scalpel-exp/procedures/)
as one Go file per procedure (closely related procedures may share a file).
Each file registers its procedure definition, parameter metadata, and
schedule-test inputs in an `init()` function; adding a procedure therefore
requires no central registry or test-table edits. Procedures schedule senders
(UDP, TCP, ICMP, QUIC) and packet captures on an `experiment.Executor`,
relative to the upcoming Starlink reconfiguration instant; the executor and
the client/server RPC layer handle execution, gathering, and results.

The full guide — procedure signature, execution modes and direction handling,
parameter types, the shared dry-run schedule tests, and private procedures
behind build tags — is in the
[procedures README](cmd/scalpel-exp/procedures/README.md).

Golden schedule snapshots live next to the procedure sources, named
`<procedure>_<mode>.golden`. After an intended schedule change, regenerate a
single snapshot and review the diff:

```sh
go test ./cmd/scalpel-exp/procedures -run '^TestProcedureSchedules/prograte_uldl$' -update
git diff cmd/scalpel-exp/procedures/
```

## Output Format

Both tools provide detailed packet and connection information in CSV format, facilitating post-hoc analysis. 

`scalpel-exp` also writes an `info.json` execution manifest for each procedure
invocation, recording its configuration, schedule, results and output files.

### Timestamp sources

Set timestamping independently when starting each client and server:

| Option           | Values                                      | Default    |
|------------------|---------------------------------------------|------------|
| `--timestamp-tx` | `hardware` (`hw`), `software` (`sw`), `app` | `software` |
| `--timestamp-rx` | `hardware` (`hw`), `software` (`sw`), `app` | `software` |

These settings apply to the **local host**, not the peer. In UL measurements the
client uses its TX setting and the server its RX setting; DL reverses those
roles. The client does not send timestamp configuration to the server or
negotiate a source. UDP results carry the timestamp evidence used in reports;
use matching client/server versions because there is no timestamp-capability
handshake.

For UDP, the requested source is required. The only implementation-only
downgrade is hardware TX to software TX, because hardware TX is not implemented.
This downgrade is logged and recorded in the timestamp report. Here, `software`
means kernel packet timestamps; `app` means timestamps captured in application
code.

Socket setup errors, unavailable NIC/driver support, and missing or invalid
packet timestamps fail the measurement. They never trigger fallback, even before
collection starts. Selection happens once, before collecting samples; the source
never changes during a run.

For example, these commands require software TX and hardware RX for UDP on both
endpoints:

```sh
./scalpel-run --timestamp-tx=software --timestamp-rx=hardware server
./scalpel-run --ip <server-ip> --timestamp-tx=software --timestamp-rx=hardware udp-periodic
```

**NetScalpel saves hardware timestamps unchanged.** It does not convert clocks,
correct offsets, verify synchronization, or access `/dev/ptpN` for calibration.
Before using hardware timestamps, enable hardware timestamping on the receiving
NIC and synchronize its PTP hardware clock to a reliable system clock externally
(for example with linuxptp's `phc2sys`, configured with the correct clock
direction). Keep both hosts' system clocks synchronized to a reliable common
time reference.

**Implementation limits:** Hardware TX timestamping is not yet implemented;
requesting it for UDP selects software TX and logs the downgrade. ICMP and QUIC
timestamp reports are deferred; no timestamp report is emitted for them. If a
selected UDP source fails, the measurement fails without trying another source.
TCP output contains sampled connection metrics rather than packet TX/RX
timestamps and has no timestamp report.

| Measurement | Timestamp report                                           |
|-------------|------------------------------------------------------------|
| UDP         | Supported (TX: software, app; RX: hardware, software, app) |
| ICMP / QUIC | Deferred; no report                                        |
| TCP metrics | No report                                                  |

Hardware socket setup can succeed even when the NIC does not produce timestamps
for the traffic. In that case, missing packet evidence still fails the run
rather than silently falling back.

### UDP Tests
UDP tests output one row per sent packet (including lost packets). The first six
columns remain `seq,ts_sent,ts_rcvd,owd_ms,size,lost`, followed by `dup_count`
and any duplicate receive timestamps:
- `seq`: Sequence number starting at 0.
- `ts_sent`: Send timestamp from the selected TX source, recorded in the optional report.
- `ts_rcvd`: Earliest arrival timestamp from the selected RX source, recorded in the optional report. Empty if lost.
- `owd_ms`: Calculated one-way delay in milliseconds, using the earliest arrival. Empty if lost.
- `size`: Packet payload size in bytes.
- `lost`: Boolean indicating if the packet was lost.
- `dup_count`: Number of extra received copies, excluding the first arrival. Always present; `0` for packets received once or lost.
- `ts_rcvd_1` through `ts_rcvd_N`: Additional arrival timestamps in chronological order, after `ts_rcvd`. `N` is the maximum `dup_count` across the file. Every row has the same number of columns; unused timestamp fields are blank. No extra timestamp columns are emitted when there are no duplicates.

For example, a file containing up to two extra copies has this header:

```csv
seq,ts_sent,ts_rcvd,owd_ms,size,lost,dup_count,ts_rcvd_1,ts_rcvd_2
```

Consumers should read the header rather than assume a fixed CSV width. Duplicate
copies do not create additional packet rows or count as additional sent packets.

### TCP Tests
TCP tests sample kernel-level metrics every 5 ms via the `tcp_info` socket option. Key fields include:
- Congestion window size (`SenderWindowSegs`)
- Packets in flight (`UnackedSegs`)
- Smoothed RTT (`RTT`)
- Retransmission counts (`TotalRetransSegs`)
- Congestion control state (`CAState`)

Transfers can be bounded by the number of bytes (`--bytes`) or duration (`--duration` in milliseconds).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
