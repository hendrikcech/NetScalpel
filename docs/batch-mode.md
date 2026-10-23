# Batch mode

Batch mode runs a validated list of existing procedures in a
chosen serial order. It does not add new measurement procedures. A procedure
round is one repetition of a procedure; a procedure invocation is one scheduled
execution within that round. Some procedures have more than one invocation per
round.

## Running a batch

Use matching client and server builds. Older result protocols are not supported;
there is no compatibility fallback.

1. Start the server on the remote endpoint: `./scalpel-exp server`.
2. Save a batch configuration as `batch.yaml` using the examples below.
3. Run the client, replacing the example IP with your server's address:

```sh
./scalpel-exp client --ip 192.0.2.10 --batch batch.yaml --results results/batch-01
```

Batch mode uses the usual connection and logging options (`--ip`, `--port`,
`--log`, and `--log-level`) and the results directory option (`--results`). It
cannot be combined with the single-procedure options `--procedure`,
`--params`, or `--rounds`, even when an explicitly supplied value matches its
default. Use `./scalpel-exp procedures` and `./scalpel-exp procedures <name>` to
check which procedures and parameters are registered in your build.

## YAML format

The root requires a `mode` (`sequential`, `shuffled`, or `interleaved`) and a
non-empty `steps` list. An optional signed 64-bit integer `seed` controls
randomized ordering; if omitted, a seed is chosen for the run. A step is either
a procedure entry with required `id`, `procedure`, and `rounds` fields and
optional `params`, or a nested block with required `id`, explicit `mode`, and
non-empty `steps`. A block
has no `rounds` field and does not inherit parameters from its parent. The full
examples below show the YAML structure.

Put procedure options in the `params` mapping, not a semicolon-separated CLI
string. Use YAML integers for numeric options and YAML lists for list options,
for example `durations: [100, 400, 1000]`. Omitted procedure parameters use their
registered defaults. Direction is special: omitting it can change how many
invocations run, as described below.

All IDs must be explicit and globally unique across the entire YAML tree,
including block IDs. IDs start with an ASCII letter or digit, contain only
ASCII letters, digits, dots, underscores, or hyphens, and are at most 64
characters long. `rounds` belongs to each procedure entry and must be a
positive finite integer. It is that entry's exact number of repetitions and is
not inherited from an ancestor. Nested entries are identified in results by
their IDs and parent-block ancestry. Unlike single-procedure `--rounds 0`, a
batch cannot request infinite repetition: `rounds: 0` is invalid.

### Ordering modes

- **`sequential`** runs direct children in the order written. A procedure entry
  completes all its rounds before the next child. A nested block runs in that
  position using its own mode.
- **`shuffled`** shuffles the direct children once. Each procedure entry's
  rounds still run together, and nested blocks remain contiguous, using their
  own modes internally.
- **`interleaved`** shuffles a pool containing one item per round of each
  direct procedure entry, plus one atomic item for each direct nested block.
  Each procedure-round item is scheduled once; a block runs completely and
  contiguously before the next pool item. A block's own mode controls its
  internal ordering.

Randomization changes serial order only; it does not run tests concurrently.
Round counts are exact repetitions, not time shares. In interleaved mode, the
same entry can appear in consecutive pool positions; no spacing or fairness
between entries is promised.

An explicit seed makes the planned order repeatable for the same configuration
and software version. It does not reproduce network conditions or randomness
inside a measurement procedure. The saved execution order is the reference for
what was actually scheduled.

## Examples

These examples use registered procedure names and parameters. Save an example
as `batch.yaml`, start `scalpel-exp server` on the remote endpoint, then run the
client command above with that server's address.

### Sequential

```yaml
mode: sequential
steps:
  - id: both-directions
    procedure: ratebidir
    rounds: 2
    params:
      duration_ms: 1000
  - id: uplink-only
    procedure: rate
    rounds: 1
    params:
      direction: ul
      duration_ms: 1000
```

### Shuffled

The children are shuffled once; the two `rate` rounds remain together.

```yaml
mode: shuffled
seed: 17
steps:
  - id: downlink
    procedure: rate
    rounds: 2
    params:
      direction: dl
      duration_ms: 1000
  - id: both-directions
    procedure: ratebidir
    rounds: 1
    params:
      duration_ms: 1000
```

### Interleaved

This runs 20 uplink rounds and 10 downlink rounds in random order. Each round
is drawn without replacement, so all 30 rounds are scheduled exactly once.

```yaml
mode: interleaved
seed: 42
steps:
  - id: rate-ul
    procedure: rate
    rounds: 20
    params:
      direction: ul
      duration_ms: 3000
  - id: rate-dl
    procedure: rate
    rounds: 10
    params:
      direction: dl
      duration_ms: 5000
```

### Nested blocks

Here the outer pool contains 21 items: 20 individual rounds of `ratebidir` and
one atomic item for the nested block. When that block is selected, it runs `B`
for 10 rounds and then `C` for 5 rounds. Its 15 rounds are not split into the
outer pool.

```yaml
mode: interleaved
seed: 23
steps:
  - id: A
    procedure: ratebidir
    rounds: 20
    params:
      duration_ms: 1000
  - id: BC-block
    mode: sequential
    steps:
      - id: B
        procedure: rate
        rounds: 10
        params:
          direction: dl
          duration_ms: 1000
      - id: C
        procedure: rate
        rounds: 5
        params:
          direction: ul
          duration_ms: 1000
```

## Directions, rounds, and invocations

UL means client to server; DL means server to client. For a per-direction
procedure such as `rate`, omitting `direction` means DL then UL
invocations, kept together within each round. Setting `direction: ul` or
`direction: dl` selects one direction; values are case-insensitive. A
procedure that runs once per round may handle omission differently: for example,
`ratebidir` covers both directions in one invocation when direction is omitted.
`trace` does not support a direction parameter. Check the help for the specific
procedure before setting parameters.

## Validation

The complete YAML tree is validated before any tests start. Validation
rejects unknown fields, unknown procedures or parameter keys, invalid
parameter values, duplicate or missing IDs, empty step lists, and invalid
round counts. This avoids starting a batch that is malformed partway through.
Use one YAML document; aliases and merge keys are not supported. Numeric
parameters and round counts must be native YAML integers, not quoted strings
or fractional numbers.

A configuration can be at most 8 MiB, with at most 16 levels of steps and
10,000 planned invocations in total. A per-direction round with omitted
direction counts as two invocations toward that limit.

## Errors and interruption

An ordinary procedure-invocation failure is recorded without an automatic
retry. The runner continues with remaining invocations and later steps
when it is safe to do so. A round is failed if any of its invocations fails; a
block aggregates failures from its contents. Cancellation or a fatal failure
(for example, inability to save results or confirm that earlier tests have
stopped) halts the batch. Cancelled and not-run work is distinguished from
failed rounds and invocations.

| Situation | What happens next |
| --- | --- |
| Connecting fails before a measurement starts | Record the failed invocation and try the next planned invocation. |
| Scheduling returns an ordinary error before any measurement starts, without a panic or persistence failure | Record the failure and continue. |
| A measurement fails, but all measurements in that invocation are confirmed finished | Record the failure and continue, including any remaining direction in that round. |
| Fetching the optional server log fails after measurement completion is confirmed | Record the failure and continue. |
| Optional server metadata is unavailable, or a version mismatch is reported | Record a warning; this alone does not fail the invocation. |
| Result retrieval fails and remote completion is unknown | Request remote cancellation and stop the batch; do not risk overlapping measurements. |
| Writing results, manifests, logs, or batch progress fails | Stop the batch and retain what could be saved. |
| Scheduling panics, or the user cancels the batch | Stop the batch; later work remains not run. |

The runner never retries an invocation or a result retrieval. Invalid batch
configuration stops validation before any measurement starts. These continuation
rules apply to batch mode; single-procedure mode still stops on its first error.
If a metadata timeout requires reconnecting and that connection fails, the
invocation fails before measurements start, and the batch continues.

Each round appears in exactly one status count. A failed invocation makes the
round failed, even if another invocation was cancelled or never ran. Otherwise,
a round with a cancelled invocation is cancelled; a round with any not-run
invocations is not run; and a round is successful only when all its invocations
succeed. For example, completing DL and then cancelling a running UL invocation
counts as one cancelled round, one successful invocation, and one cancelled
invocation.

Press Ctrl-C once to request a graceful stop and retain partial results. A
second signal forces the process to exit and can prevent final summary updates.
Running the same file again starts a new batch from the beginning; it does not
resume an interrupted batch or skip previously successful rounds.

## Results and summary

Each run creates a separate batch directory beneath `--results`, so running
another batch does not replace earlier summaries. The command prints the batch
directory. It contains:

- `batch-plan.json`: the seed, software build, resolved parameters, entry and
  block identities, and the full planned invocation order, saved before any
  measurements start.
- `batch-summary.json`: per-invocation outcomes, result paths and errors, and
  round and invocation counts for each entry, each block, and the whole batch.
  This file is updated during execution and finalized when the batch stops.
- Separate invocation directories with the usual measurement files and
  `info.json` manifests, including their batch and entry identity.

Invocation directories use `<timestamp>_<procedure>_<direction>`, for example
`20261023T131812_rate_dl`. Direction is omitted when it is not a procedure
parameter. The batch ID is not repeated in these names. If a name already
exists, a short suffix (`_1`, `_2`, and so on) is added; existing results are
never overwritten. Execution positions remain in the plan, summary, and
invocation manifests rather than directory names.

Round numbers identify repetitions within an entry; they are not global
execution positions. Interleaving can execute those round numbers out of order.
Use `execution_position` in the plan and summary to follow the actual ordering.

At the end, the command prints and saves a summary with successful and failed
round and invocation counts for each procedure entry and overall. The summary
includes block ancestry; the saved files include error details and result paths.
Together, the plan and summary record the order, seed, resolved procedure
parameters, and software version to help reproduce a run. A batch with failures
exits nonzero. Successful, failed, cancelled, and not-run work are counted
separately; success means the invocation finished and its results were collected, not that
the measured network had no packet loss.

## Timing and measurement effects

Batch execution retains the existing RI (reconfiguration-instant)
scheduling and result-gathering behavior. Batch items run serially,
without overlap; randomization only changes order and does not add parallel
execution. Batch mode does not add a cooldown. Measurement state such as
queues or network load can carry over between items, and serial order can bias
comparisons, so random ordering does not by itself remove time-dependent
effects. Batch mode does not include block repetition, parameter
inheritance, retries, resuming, or parallel execution. Serial ordering applies
only to this batch; other clients or background traffic can still interfere
with measurements.
