# UDP pacing policy and evaluation

## Current policy: preserve cumulative count

RateSender targets `floor(elapsed * pps)` packets within each segment. A late
tick pays the entire outstanding debt, subject to cancellation. The 1024-packet
sendmmsg batch size bounds a syscall, **not** a catch-up burst: consecutive batches
can be sent without a pacing delay. The nominal five packets per tick likewise
does not cap bursts after a scheduler stall. At segment end, unsent debt expires;
the sender does not extend the measurement to reach the requested count.

Keep this behavior as the default. Silently capping debt would change the offered
load and make results from old and new clients less comparable. Reducing padding,
decoding and allocation overhead is independent of this policy.

## Bounded catch-up alternatives

For a 100,000 pps segment stalled for 20 ms, 2,000 packets are due. With a
100-packet catch-up allowance per 1 ms service opportunity:

| Policy | First service opportunity | Outstanding debt | Consequence |
| --- | ---: | ---: | --- |
| Current cumulative count | 2,000 | 0 | Large short-window burst |
| Cap service, retain debt | 100 | 1,900 | At target service capacity, debt never clears |
| Cap debt, discard excess | 100 | 0 (1,900 discarded) | Stable windows, permanent offered-load deficit |

These numbers are deterministic policy arithmetic, not kernel measurements;
`TestCatchUpPolicyTradeoff` checks them. Simply limiting a sendRound invocation
does not define a short-window bound: debt could be drained by repeated immediate
calls. A future opt-in bounded mode must specify both a time window and its packet
budget, when tokens refill, whether excess debt is discarded or deferred, and what
happens at segment transitions/cancellation. Record discarded demand separately
from packets actually sent; do not invent lost sequence numbers for unsent demand.

## Separate kernel pacing experiment (not enabled)

Evaluate SO_TXTIME with a compatible Linux FQ qdisc on a dedicated test interface,
not by changing the host's default qdisc from NetScalpel. The experiment must:

1. Verify kernel/qdisc support and privilege requirements; reject unsupported
   configurations explicitly. Confirm that FQ honors transmit times on that
   kernel/device, rather than assuming a socket option alone provides pacing.
2. Use a supported clock (e.g. CLOCK_MONOTONIC where supported), absolute per-packet
   transmit times derived from the segment epoch, and bounded scheduling lookahead.
   CLOCK_MONOTONIC scheduling times must not be confused with the realtime software
   TX timestamps used in result records.
3. Specify late-packet handling (drop vs immediate send), queue horizon, segment
   transitions and cancellation. Packets already queued in the kernel may outlive
   a cancelled Go context; a working implementation must prevent post-test traffic.
4. Distinguish SO_EE_ORIGIN_TXTIME scheduling errors from
   SO_EE_ORIGIN_TIMESTAMPING notifications on the shared error queue. Count/report
   late or invalid scheduling drops without treating them as successful sends.
5. Compare against userspace pacing and both bounded-debt policies at 1k, 10k,
   100k pps and near saturation, with small/MTU packets, segment changes, idle
   segments, and controlled 1/10/20 ms CPU stalls. Include long runs and both IPv4
   and IPv6. Use actual TX timestamps, not sendmmsg return times, for spacing.
6. Report total requested/sent/received/dropped packets, per-1 ms and per-10 ms
   maximum counts, inter-departure p50/p99/max, deadline overrun, CPU time,
   allocations and peak RSS. Record kernel, qdisc, NIC offloads and timestamp-loss
   counts. Reject an apparent stability win caused only by sending fewer packets.

No kernel pacing result is claimed here. Selecting a new default requires these
measurements and an explicit decision about count accuracy versus window stability.
