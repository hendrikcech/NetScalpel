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
./scalpel-exp procedures
```

These procedures include advanced scenarios like `MultiDurationRate`, `Burst`, `Cooldown`, `SwitchFlow`, `Rate`, `OWD`, `MouseElephant`, and `TCPReconf`.

To run an orchestrated experiment, you typically start the `scalpel-exp server` on the remote endpoint and the `scalpel-exp client` on the local endpoint, passing the necessary flags for the desired procedure.

## Implementing New Experiments

The `scalpel-exp` experiment framework is designed to be extended with new procedures. This section explains the architecture and walks through adding one.

### Architecture Overview

```
CLI (main.go)
 └─ Client (client.go)         Orchestrates rounds, dials RPC, aligns to RI
     └─ ProcedureFunc           Builds the schedule: which senders start when
         └─ Executor            Collects scheduled clients, runs them concurrently
             ├─ SenderClient    A single network test (UDP, TCP, ICMP, QUIC)
             └─ CommandClient   A local/remote command (e.g. tcpdump)
```

A **procedure** is a plain Go function that schedules one or more measurement tasks onto an `Executor`. It does not run traffic itself — it declares *what* to send, *when*, and *where to store results*. The executor and the client/server RPC layer handle the rest.

### The `ProcedureFunc` Signature

Every procedure has this signature (defined in `cmd/scalpel-exp/client.go`):

```go
type ProcedureFunc func(e *Executor, ts time.Time, resultPath string, params ParamMap) error
```

| Parameter    | Purpose |
|:-------------|:--------|
| `e`          | The executor to schedule tasks on via `e.RunClient(...)`. |
| `ts`         | The synchronized start time, aligned to the next Starlink reconfiguration interval (RI). All scheduled `StartAt` times should be relative to `ts`. |
| `resultPath` | Pre-created directory for this round's output files. |
| `params`     | Key-value parameters from the CLI `--params` flag (e.g. `direction=ul;durations=100,200`). |

### Available Senders

Procedures compose experiments from these building blocks defined in the `pkg` package:

| Sender | Params Struct | What it does |
|:-------|:-------------|:-------------|
| `BurstSender` | `BurstParams{Timeout, Num, Pad}` | Sends `Num` UDP packets at line rate. Used to probe queue capacity. |
| `RateSender` | `[]RateParams{Pps, Interval, Duration, PayloadSize}` | Sends UDP packets at a fixed rate. Accepts a slice of phases to model send/pause/send patterns. |
| `PeriodicSender` | `PeriodicParams{Interval, Duration, Pad}` | Sends one UDP packet per interval. Lightweight OWD probe. |
| `TCPSender` | `TCPSenderParams{Duration_, Bytes, CCA}` | Opens a TCP flow, optionally bounded by duration or bytes, with a configurable congestion control algorithm. |
| `QUICSender` | `QUICParams{Duration_, Bytes}` | QUIC transfer bounded by duration and/or bytes. |
| `ICMPSender` | `ICMPParams{Interval, Duration_}` | Sends ICMP echo requests at a constant interval. |

Each sender is wrapped in a `pkg.SenderClient` that specifies the target IP, output path, direction, and start time.

### Procedure Registries

Procedures are registered in one of two maps in `cmd/scalpel-exp/procedures.go`:

- **`proceduresUlDl`** — Direction-aware procedures. If the user omits `--params "direction=..."`, the framework automatically runs the procedure twice (once UL, once DL).
- **`proceduresBidir`** — Procedures that manage direction internally or are inherently bidirectional. Called once per round regardless.

### Step-by-Step: Adding a New Procedure

**1. Write the procedure function** in `cmd/scalpel-exp/procedures.go`:

```go
func DrainQueue(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
    // 1. Extract direction from params (required for proceduresUlDl entries).
    direction, err := params.Direction()
    if err != nil {
        return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
    }

    // 2. Read optional params with defaults.
    drainMs, err := params.UintOr("drain_ms", 500)
    if err != nil {
        return err
    }

    // 3. Compute timing. All times are relative to ts (the RI boundary).
    start := ts.Add(1 * time.Second)
    deadline := nextRi(ts).Add(-time.Second) // end of the current RI window
    duration := time.Duration(drainMs) * time.Millisecond

    if start.Add(duration).After(deadline) {
        return fmt.Errorf("test does not fit in the current RI")
    }

    // 4. Schedule a burst to fill the queue.
    e.RunClient(&pkg.SenderClient{
        IP:        e.IP,
        Out:       filepath.Join(resultPath, fmt.Sprintf("burst_%v.csv", direction.StringLower())),
        Direction: direction,
        StartAt:   start,
        Sender: &pkg.BurstSender{Params: pkg.BurstParams{
            Timeout: 4 * time.Second,
            Num:     5000,
            Pad:     1400,
        }},
    })

    // 5. After a pause, probe OWD to observe the queue draining.
    probeStart := start.Add(duration)
    e.RunClient(&pkg.SenderClient{
        IP:        e.IP,
        Out:       filepath.Join(resultPath, fmt.Sprintf("owd_%v_%04d.csv", direction.StringLower(), drainMs)),
        Direction: direction,
        StartAt:   probeStart,
        Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
            Interval: time.Millisecond,
            Duration: 3 * time.Second,
        }},
    })

    // 6. Capture packets spanning the full test window.
    e.tcpdump(resultPath, ts, probeStart.Add(3*time.Second).Sub(ts)+time.Second)

    return nil
}
```

**2. Register it** by adding an entry to the `proceduresUlDl` map (or `proceduresBidir`):

```go
var proceduresUlDl = map[string]ProcedureFunc{
    // ... existing entries ...
    "drainqueue": DrainQueue,
}
```

**3. Add test coverage** in `cmd/scalpel-exp/procedures_test.go`. Every registered procedure must have an entry in the dry-run parameter table — the test will fail otherwise:

```go
var uldlProcedureParams = map[string]ParamMap{
    // ... existing entries ...
    "drainqueue": {"direction": "ul"},
}
```

Then generate the golden snapshot and review the diff:

```sh
go test ./cmd/scalpel-exp -run TestProcedureSchedules -update
git diff cmd/scalpel-exp/testdata/
```

The dry-run tests validate schedule invariants automatically: `StartAt` bounds, unique output paths, and tcpdump coverage of all sender windows.

### Key Conventions

- **Fit within one RI.** Most procedures compute a `deadline` from `nextRi(ts)` and verify all scheduled work completes before it. Panic or return an error if the schedule overflows.
- **Randomize iteration order.** Use `rng.Perm(len(items))` to shuffle parameter combinations across rounds. This prevents ordering artifacts in measurement data.
- **Use descriptive filenames.** Encode variable parameters (direction, rate, duration) into the CSV filename so analysis scripts can parse them: `rate_ul_140_4000.csv`.
- **End with `e.tcpdump(...)`.** Every procedure should capture packets for the duration of the test.
- **Use `ParamMap` for tunables.** Expose knobs like durations, rates, or packet counts through `params.UintsOr(key, defaults)` so they can be overridden from the CLI without code changes.

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
