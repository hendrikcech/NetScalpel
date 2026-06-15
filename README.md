# NetScalpel
A composable measurement toolkit to surgically dissect link and queue dynamics

## Overview

NetScalpel is a custom network measurement tool designed for fine-grained control over packet transmission timing and scheduling. Written in Go, it utilizes a client-server architecture to coordinate test execution and exchange results over a TCP control channel. The toolkit leverages kernel-level timestamps to record per-packet send and receive times with microsecond precision, enabling accurate computation of one-way delay (OWD), packet loss, and receive rates. Accurate analysis requires NTP synchronization between the client and server endpoints.

## Citation

NetScalpel was developed and used for the measurements presented in the paper *Dissecting the StarLink: Characterizing Queuing and Flow Dynamics in the Starlink Network*, published at SIGCOMM'26. 

If you use this tool in your research, please cite our paper:

<details>
<summary>BibTeX</summary>

```bibtex
@inproceedings{cech2026starlink,
  title={Dissecting the StarLink: Characterizing Queuing and Flow Dynamics in the Starlink Network},
  author={Cech, Hendrik and Mohan, Nitinder and Ott, J\"{o}rg},
  booktitle={Proceedings of the 2026 ACM SIGCOMM Conference},
  year={2026}
}
```

</details>

## Building the Toolkit

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
./scalpel-exp procedures
```

These procedures include advanced scenarios like `MultiDurationRate`, `Burst`, `Cooldown`, `SwitchFlow`, `Rate`, `OWD`, `MouseElephant`, and `TCPReconf`.

To run an orchestrated experiment, you typically start the `scalpel-exp server` on the remote endpoint and the `scalpel-exp client` on the local endpoint, passing the necessary flags for the desired procedure.

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
