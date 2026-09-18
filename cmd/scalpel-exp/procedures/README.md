# Experiment procedures

An experiment is one complete `scalpel-exp client` run. It selects a named,
reusable procedure and repeats it for the requested number of rounds. A
procedure invocation is one scheduled execution within a round; a
`PerDirection` procedure may have separate DL and UL invocations.

Procedures compose NetScalpel senders and captures. Add a Go file in this
directory to add a procedure to the next build. Each file registers its
procedure, supported parameters, and schedule-test inputs.

## Package structure

- `procedures`: procedure definitions, registration, parameters, and golden tests.
- `../experiment`: shared `Executor`, `ParamMap`, and RI timing helpers.
- The parent command: CLI, direction expansion, rounds, and result handling.

Go files in this directory use `package procedures`. They import the shared
`experiment` package rather than the parent command, which is not importable.

## Defining a procedure

A procedure has this signature:

```go
func PrivateProbe(
    e *experiment.Executor,
    ts time.Time,
    resultPath string,
    params experiment.ParamMap,
) error
```

- `e` provides the server IP and schedules clients through `RunClient`.
- `ts` is the reference Starlink reconfiguration instant (RI), not necessarily now.
- `resultPath` is the output directory for this invocation.
- `params` contains parsed overrides and ordinary parameter defaults. Direction
  is handled separately as described below.

Use `e.RunClient` for each sender or command and `e.Tcpdump` for paired local and
remote captures. In a normal run, RunClient starts client execution asynchronously;
in a dry run, it records clients without executing them. Keep scheduling code
compatible with dry runs: avoid direct network calls, sleeps, and independent
goroutines. The runner waits for clients and gathers results.

### Example

Create `privateprobe.go`:

```go
package procedures

import (
    "fmt"
    "path/filepath"
    "time"

    "github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
    "github.com/hendrikcech/netscalpel/pkg"
)

func init() {
    Register(Procedure{
        Name:        "privateprobe",
        Description: "Measure periodic UDP one-way delay.",
        Mode:        PerDirection,
        Run:         PrivateProbe,
        Params: []ParamSpec{
            {
                Name:        "duration_ms",
                Description: "Probe duration in milliseconds.",
                Default:     uint(1000),
            },
        },
        ScheduleTest: &ScheduleTest{
            Params: experiment.ParamMap{"direction": "ul"},
        },
    })
}

func PrivateProbe(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
    direction, err := params.Direction()
    if err != nil {
        return err
    }
    durationMs, err := params.Uint("duration_ms")
    if err != nil {
        return err
    }
    if durationMs == 0 {
        return fmt.Errorf("duration_ms must be positive")
    }
    duration := time.Duration(durationMs) * time.Millisecond
    start := ts.Add(time.Second)
    e.RunClient(&pkg.SenderClient{
        IP:        e.IP,
        Out:       filepath.Join(resultPath, "owd_"+direction.StringLower()+".csv"),
        Direction: direction,
        StartAt:   start,
        Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
            Interval: time.Millisecond,
            Duration: duration,
            Pad:      0,
        }},
    })
    e.Tcpdump(resultPath, ts, duration+2*time.Second)
    return nil
}
```

No central name list or per-procedure `_test.go` file is needed. Duplicate
procedure names are registration errors. Related procedures may share a source
file and implementation.

## Execution modes and direction

| Mode | Direction omitted | Direction supplied |
| --- | --- | --- |
| `PerDirection` | Separate DL then UL invocations per round | One invocation for the selected direction |
| `OncePerRound` | One invocation per round | Accepted only with `SupportsDirection: true` |

PerDirection automatically supports `direction`; do not add it to Params.
OncePerRound procedures that explicitly support direction set SupportsDirection.
For example, `owdbidir` and `ratebidir` interpret omission as measuring both
directions within one invocation. `trace` does not accept direction.

Direction has no ordinary default: omission controls execution behavior. Accepted
values are `ul` and `dl`, case-insensitively.

## Parameters and help

Each ParamSpec has three fields: Name, Description, and Default. Its default's Go
type determines how an override is parsed:

| Default type | CLI value example |
| --- | --- |
| `uint` | `duration_ms=1000` |
| `[]uint` | `durations=100,400,1000` |
| `[]pkg.TCPCCA` | `ccas=cubic` (names recognized by `pkg.ParseTCPCCA`) |
| `string` | `label=baseline` |
| `[]string` | `labels=baseline,probe` |

Use explicit types: `uint(50)`, not `50`, and `[]uint{100, 400}`, not untyped nil.
A single CLI value is accepted for a list parameter. Supported typed empty slices
also carry type information. Defaults and supplied slices are copied per invocation.

There is no Required field or general bounds framework. Choose usable defaults;
check procedure-specific requirements before scheduling clients.
Unknown keys and values that cannot be parsed are rejected before connecting to
the measurement server. Parameter descriptions should state units and meaning.

Defaults belong in registration, not duplicated in the procedure implementation.
Read prepared values using accessors such as Uint, Uints, and TCPCCAs.

From the repository root:

```sh
go build -o scalpel-exp ./cmd/scalpel-exp
./scalpel-exp procedures
./scalpel-exp procedures privateprobe
./scalpel-exp client --ip 192.0.2.1 --procedure privateprobe --params 'direction=ul;duration_ms=2000'
```

Replace the example IP with your measurement server. Quote semicolon-separated
parameters so the shell passes them as one argument. Procedure help lists its
description, mode, supported parameters, inferred types, and defaults.

## Schedule tests and golden files

Every procedure registers a non-nil ScheduleTest. Its Params are **test overrides**,
not runtime defaults. Use `&ScheduleTest{}` if no overrides are needed; for a
PerDirection procedure, specify a test direction. Long-running procedures can
use shorter test durations, as existing OWD and rate tests do.

The shared test harness:

1. Applies the same parameter parsing and defaults as normal execution.
2. Supplies a fixed RI anchor and deterministically seeded procedure RNG.
3. Invokes the procedure with Executor.DryRun, without network traffic or sleeps.
4. Checks that clients were scheduled, output names are nonempty and unique,
   starts are sensible, and scheduled captures cover sender windows.
5. Normalizes and sorts the schedule, then compares it to a golden snapshot.

Use the package `rng` for randomized scheduling so tests can reproduce it. Avoid
wall-clock dependence when the supplied ts is sufficient. Schedule tests sharing
the RNG run serially. Golden tests verify scheduling, not packet transmission
or measurement accuracy.

### Naming

The filename is `<base>_<mode>.golden`, with mode `uldl` or `bidir`. The default
base is the registered name:

```text
privateprobe.go
privateprobe_uldl.golden
```

For a shared implementation, set ScheduleTest.GoldenBase to group fixtures:

```go
// Within the "owdbidir" registration:
ScheduleTest: &ScheduleTest{
    GoldenBase: "owd",
    Params: experiment.ParamMap{"duration_ms": "3000"},
},
```

This gives `owd.go`, `owd_bidir.golden`, and `owd_uldl.golden`. GoldenBase is only
a basename, not a path. Registered procedures must not share a resolved golden
filename. Snapshot `# params:` headers record test overrides; execution also
uses the declared defaults.

For a procedure intentionally scheduling before ts, set
`ScheduleTest.AllowBeforeStart: true`. This enables the harness's existing limited
pre-anchor allowance; do not use it to suppress unintended scheduling errors.

### Adding or updating a snapshot

From the repository root:

```sh
go test ./cmd/scalpel-exp/procedures -run '^TestProcedureSchedules/privateprobe_uldl$' -update
go test ./cmd/scalpel-exp/...
```

Review the generated snapshot before accepting it. Check flow directions, start
offsets, durations, output filenames, and capture coverage. Updates must not bypass
invariant failures. For all intentional snapshot updates, use:

```sh
go test ./cmd/scalpel-exp/procedures -run '^TestProcedureSchedules$' -update
```

Add a separate `_test.go` file only for behavior the shared harness does not cover.

## Private procedures

Keep private source and fixtures in private storage, and copy them into this
directory in your build checkout. Optionally start a private Go file with:

```go
//go:build privateexperiments

package procedures
```

Then build and test with the same tag:

```sh
go build -tags privateexperiments -o scalpel-exp ./cmd/scalpel-exp
go test -tags privateexperiments ./cmd/scalpel-exp/...
go test -tags privateexperiments ./cmd/scalpel-exp/procedures -run '^TestProcedureSchedules/privateprobe_uldl$' -update
```

The fixture itself needs no build tag: the harness tests only registered
procedures. Build tags select compilation; they do not make checked-in files
private. Rebuild the executable after adding or modifying procedure Go files.

Procedures composing existing senders normally need no server changes. New
measurement primitives may also require RPC, serialization, and server support;
procedure registration alone does not supply those capabilities.
