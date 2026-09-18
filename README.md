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

NetScalpel is a custom network measurement tool designed for fine-grained control over packet transmission timing and scheduling. Written in Go, it utilizes a client-server architecture to coordinate test execution and exchange results over a TCP control channel. The toolkit leverages kernel-level timestamps to record per-packet send and receive times with microsecond precision, enabling accurate computation of one-way delay (OWD), packet loss, and receive rates. Accurate analysis requires NTP synchronization between the client and server endpoints.

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

The `scalpel-exp` binary is used to orchestrate complex, predefined experiments that schedule the execution of specific UDP or TCP measurements ahead of time. This prevents mixing measurement and control traffic during the experiment.

To view the list of supported experimental procedures:

```sh
./scalpel-exp procedures          # all registered procedures
./scalpel-exp procedures prograte # details for one procedure
```

These procedures include advanced scenarios like `MultiDurationRate`, `Burst`, `Cooldown`, `SwitchFlow`, `Rate`, `OWD`, `MouseElephant`, and `TCPReconf`.

To run an orchestrated experiment, you typically start the `scalpel-exp server` on the remote endpoint and the `scalpel-exp client` on the local endpoint, passing the necessary flags for the desired procedure.

## Implementing New Experiments

Experiment procedures live in [`cmd/scalpel-exp/procedures`](cmd/scalpel-exp/procedures/)
as one Go file per procedure (closely related procedures may share a file).
Each file registers its procedure definition, parameter metadata, and
schedule-test inputs in an `init()` function; adding an experiment therefore
requires no central registry or test-table edits. Procedures schedule senders
(UDP, TCP, ICMP, QUIC) and packet captures on an `experiment.Executor`,
relative to the upcoming Starlink reconfiguration instant; the executor and
the client/server RPC layer handle execution, gathering, and results.

The full guide — procedure signature, execution modes and direction handling,
parameter types, the shared dry-run schedule tests, and private experiments
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

### UDP Tests
UDP tests output information about each individual packet:
- `seq`: Sequence number starting at 0.
- `ts_sent`: Timestamp taken when the packet is sent (via kernel software transmission timestamps).
- `ts_rcvd`: Timestamp when the packet was received by the server (via Linux receive timestamping). Empty if lost.
- `owd_ms`: Calculated one-way delay in milliseconds.
- `lost`: Boolean indicating if the packet was lost.

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
