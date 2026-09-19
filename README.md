# xmip-test-playground

**The Xmip Playground.** One integration test — the **RoundTrip test** — over
the whole estate, over time.

Its scenario is a round trip: send a payload, catch it, check it came back
whole. It runs that over every transport by every content contract, on a
Schedule, and never stops. Each round folds into a running tally per pair, so a
pair is judged by its record over time — one failure among thousands stays
visible until a round passes again. Every pair rolls up to one state at
`xmip:///<node>/exercise`, so an operator sees one green or the one pair that
broke. ADR-0028.

RoundTrip is the scenario, not a protocol; the transport is what varies under
it. Xmip's own transports are both ends, so nothing external is stood up.

## The scenarios

Moved here from ADR-0028 on 2026-09-12; the record keeps the decisions.


RoundTrip is the first test, not the only one. Each scenario asks a different
question of the same estate over the same [`RoundTrip`] adapters, and publishes
under its own subtree of `xmip:///playground`, merged into one snapshot so the
rollup covers them all and an operator drills scenario → detail → the failing
leaf. 2026-09-19, the owner: the old scenario wording is replaced by the test
names everywhere; the scope segment is the name in kebab case (`round-trip`,
`low-latency`, `heavy-load`, `retention`, `filing`, `exclusive-claim`,
`daily-backlog`):

- **RoundTrip** — did it arrive whole and hold its contract, across the stages.
- **LowLatency** — did it arrive in time: round-trip latency against a per-transport
  budget, judged on the p50/p99 of recent rounds (cold-start rounds skipped).
- **HeavyLoad** — a large payload per pair (a megabyte by default, **gigabytes** on
  demand): did it arrive byte-for-byte and, below a parse ceiling, still validate
  at size; and at what throughput. Above the ceiling the structural contract is
  not parsed — a gigabyte parse allocates a second copy and proves nothing the
  byte check does not — so the claim is byte integrity at scale. A UDP datagram
  cannot hold a megabyte, and that real ceiling shows as red with no injection.
  Peak memory is roughly twice the size per pair, pairs run one at a time; true
  multi-gigabyte without that doubling wants a streaming round trip, queued.
- **Retention** — retention and archiving: retain, then archive by age, driving
  the estate's real `RetentionPolicy` and `ArchiveStore` over a logical clock; a
  missed sweep under pressure surfaces as a retention leak. There is no third
  act — Xmip retains and archives, it does not delete (ADR-0040).
- **Filing** — *added 2026-09-09.* Does the archive hold what it was handed:
  one probe item per contract filed through every archive technology on main
  — parquet, sqlite, file, sql, postgresql, mssql, mysql, s3, azure-blob, gcs —
  archived and restored, judged equal or not, under
  `xmip:///playground/filing/<technology>/<contract>`. Retention proves the
  lifecycle over one store; Filing proves every store. Each technology gets a
  `Cabinet` adapter the way each transport gets a `RoundTrip`, so a new archive
  technology is a new adapter, not a new scenario. Under pressure a filing is
  skipped now and then and reported as a fault.
- **storm** — *added 2026-09-09.* Every transport by every contract at a stress
  level, many pairs at once, harsh faults, the level's payloads cycling by
  round — and its subject is the invariants that must survive that: a tick
  finishes within its bound, every failure carries a reason, a red leaf
  reaches the root, nothing panics. RoundTrip proves the pair; storm proves the
  playground and the estate under it do not lie when leaned on.
- **ExclusiveClaim** — exclusive pickup: a dropped item is read by exactly one holder,
  under real thread contention, across the **execution style** it declares —
  Sequential, Parallel, Concurrent (runtime-model.md). Sequential additionally
  keeps order per key. The claim is the estate's (ADR-0024), taken by atomically
  creating a lock (`create_new`/`O_EXCL`, which is exclusive under concurrency
  where a rename to a per-reader name is not). Under pressure the atomic claim is
  dropped and a second reader takes the same item — a `Contended` red, the
  duplicate-pickup bug. It runs over the file substrate; **no protocol is named
  in the code** (the owner's rule) — any other pollable transport joins by adding
  a `RoundTrip` adapter, which is when SFTP, FTPS and FTP get this exercise.
- **DailyBacklog** — a day's backlog drained as fast as possible: many files arrive at
  once and a node clears what its capacity allows. When arrivals outpace it the
  backlog climbs and the scenario escalates as an operator would — first a
  **tweak** (raise the node's concurrency), then, if that only slows the rise,
  **add a node** to share the backlog through the claim. The board shows the
  backlog climb, names the action taken, and shows it fall. The backlog is real
  files on disk, so the queue depth is a real count.

Each injects its own pressure (faults, latency spikes, dropped transfers, missed
sweeps, dropped claims) so the board is realistic rather than uniformly green;
`file` is left clean in every one.

## Two time limits bound any roll

Every roll honors two limits, one `Budget` shared by all scenarios rather than
per-scenario knobs. A **maximum time** is a wall-clock ceiling: when it is
reached the roll stops, whatever the round count — `XMIP_PLAYGROUND_MAX_SECONDS`.
The ceiling is checked between rounds, so a long tick runs to completion rather
than being cut mid-round; the maximum bounds how long a run lasts, not how long
a single round takes.

A **factor on time** stretches a **simulated clock** against real time:
`1.0` mimics real time — one simulated second per real second — and *retracting*
it below one runs simulated time faster than real, so a long horizon plays out in
a short run. Fifteen real minutes over three simulated years is `MAX_SECONDS=900`
with `TIME_FACTOR ≈ 9.5e-6` (900 real seconds ÷ three years) —
`XMIP_PLAYGROUND_TIME_FACTOR`. The round cadence stays real; the factor stretches
*simulated* time, not the wait between rounds. Scenarios that age on a clock —
the Retention test's lifecycle, retained 90 days then archived — read
simulated elapsed from the `Budget`, so an operator watches records born, live
out their retention, and cross into the archive over a horizon far longer than
the run. Rate- and latency-based scenarios ignore it; they answer in real time.

## Difficulty

### The stress level


Loopback never fails, one payload never surprises, one round at a time never
contends. A **stress level** — `calm`, `realistic`, `harsh`, `brutal` — turns
each of those up together, and every scenario takes the level rather than its
own idea of hard: the fault rates (none, as written, tripled, at the ceiling of
nine rounds in ten), the payload sizes (a few bytes; then the sizes protocols
break on — a datagram's MTU either side, the UDP maximum, sixty-four kibibytes
plus one, a mebibyte), how many pairs run at once (one, one, four, every core),
how many rounds a test drives, and how many node processes a roll spawns (one,
three, ten, forty). `realistic` is what the runner ran at before the axis
existed and is the default, so nothing changed quietly; `XMIP_PLAYGROUND_STRESS`
sets it for a roll.

### Every transport declares its ceiling

A `RoundTrip` adapter answers `ceiling()`: the largest payload its protocol
carries whole in one round, or none. Every adapter is judged on the edge
payloads — empty, one byte, every byte value, a NUL run, high bytes, a CRLF
storm, and the sizes above filled with a pattern a truncation or reorder would
show: under the ceiling they come back whole, above it the adapter refuses with
a reason, and no round exceeds three times the timeout. A ceiling is a fact
about the protocol, written where it comes from; it is never set to make a test
pass.

### Tests at every level

Every scenario keeps its calm tests and gains a `harsh` test over three
transports for the level's rounds, asserting the scenario's own invariant under
faults, contention and the edge payloads, and an ignored `brutal` test over the
whole matrix for the runner to fire. The default suite stays a suite — minutes,
not hours; the brutal runs are what a roll is for.

### The tests use half of what is free when they start, 2026-09-11

The stress level sized `brutal` to every core and to forty node processes,
`harsh` to ten. The owner ruled on 2026-09-11, watching a brutal roll: the
tests may use half of the resources left when they start. A machine a quarter
busy has three quarters free; the Playground takes half of that, three eighths
of the machine, and the other half of what was free stays with whatever else
the machine is doing — and that other work moves, so the measure is taken
again before every round. `headroom.rs` reads processor time less what the
roll and its nodes burn themselves — the Windows performance counters,
`/proc/stat` on Linux, free assumed elsewhere — and every count that would
take the whole machine is scaled to that budget, never below one: `brutal`
drives pairs from the budgeted cores and spawns forty nodes' worth of budget,
`harsh` four cores and ten nodes' worth. The nodes are counted when they are
spawned; the pairs follow the budget round by round, and the roll prints the
budget beside each round.

### The far end moved into the transport, 2026-09-11

Clause 5 and *Every transport declares its ceiling* above are read through
ADR-0051 since 2026-09-11: the dance that makes a transport its own far end,
its ceiling and its refusals are written in the technology's crate as the
capability's `Loopback`, and the Playground drives every transport through
one adapter over it. "A new transport is a new adapter, not a new scenario"
becomes "a new transport is its own loopback, and a line in the list".

### Nodes are processes, now

Decision 2 said nodes run as System Processes, and until this day no scenario
spawned one. The **cluster** does (`cluster.rs`): `node`, a second binary, is
one emulated node that runs its part of the tests over a directory the whole
cluster shares and publishes its own snapshot under
`xmip:///<cluster>/node/<name>`; the cluster spawns one per name, or the level's
count of them, merges their snapshots each round, adds the rollup the surface
owes at `xmip:///<cluster>/node` (ADR-0027 decision 8), and kills and restarts a
node whose snapshot stops moving — a recorded yellow, never silent. Exclusive
pickup and backlog draining are thereby contended by real processes, which is
the property ADR-0024's claim exists to prove and a thread could only imitate.
`XMIP_PLAYGROUND_NODES` or a harsh or brutal level puts the nodes on the board
beside the in-process scenarios. What the cluster knows of a node's process —
alive, exited, restarted — is at `node/<name>/system-process`.

### Even clusters are spawned as processes, 2026-09-19

The owner: *even clusters have to be spawned as processes during tests.* Until
this day `xmip-playground-roll` was both the test and the cluster. Now the
tree is three deep, and each of the three declares its name, location and
purpose Test where it starts (ADR-0053):

```text
xmip-playground-roll             the test: chooses scenarios, sets stress, judges, draws
└─ xmip-playground-cluster C1    the cluster: owns the shared store, spawns and watches
   ├─ xmip-playground-node R1    the nodes, one process each
   ├─ xmip-playground-node P1
   └─ xmip-playground-node S1
```

The **roll** keeps what a test driver owns: the scenarios that stay in its own
process (LowLatency, HeavyLoad, Retention, Filing, and RoundTrip when there
are no nodes declaring a stage), the board, the history, the activity, and
`<cluster>-snapshot.toml` — the one file the prompt, the CLI and the web GUI
read, at the same path and in the same shape as before. It spawns exactly one
cluster process when nodes are named, merges the file that cluster publishes
into its own each round, and stops it when the rounds run out.

The **cluster** (`src/bin/cluster.rs`) is a [`Cluster`] and nothing else. It
takes `--name --shared --nodes --stress --rounds --snapshot`, with
`--online`, `--scenarios` and `--interval-ms` optional; a value it cannot read
is REFUSED with exit code 2, naming what was wrong and what would be right
(ADR-0055). Its scope root is `XMIP_PLAYGROUND_CLUSTER`, which the roll sets
on it and which its own nodes inherit, so all three agree without being told
twice — and `--name` must be that same word. It publishes
`<cluster>-cluster.toml` beside the roll's snapshot, atomically, each round,
exactly as a node publishes its own.

`stop` in the shared directory stops the whole tree: a node leaves between
rounds, and so does its cluster, which stops its own nodes before it goes.
`Stop-XmipTest` ends a run from the leaves up — nodes, then the cluster, then
the roll — so `Get-Process xmip-*` is empty afterwards and nothing is
orphaned. `Get-XmipProcess` shows all three kinds with the location and
purpose each declared, and `Get-XmipTestNode` reports a node's roll, which is
now its grandparent.

### A cluster and its nodes: a node declares what it can do, 2026-09-19

The owner: *Fleet is what I see in topology when running test, I would like to
see cluster, nodes, receive, process, send.* What the Playground spawns is a
cluster and its nodes, and the word it used until then is retired (ADR-0028).

**A node declares what it can do** (`capability.rs`, `roster.rs`; ADR-0056). A
node is started with a capability — `--can receive`, `--can process,send` —
and serves the stages of the message path it declared, no more and no fewer. A
node that declares none behaves as every node did before: it runs the
shared-directory tests whole and no part of RoundTrip. Two of ADR-0056's four
kinds are modelled here, feature capability (the stages) and online capability
(`--online`, ADR-0045); authentication and runtime capability are not, and the
node's own capability record says so rather than leaving it to be guessed.

The rig read a node's stage out of the first letter of its name for one
afternoon on 2026-09-19, until the owner said *I know, so why do you break
it!* — ADR-0009 already had it that what a node does is its configuration, and
ADR-0022 that placement must satisfy node capability. **Nothing at runtime
reads a node's name.** The one exception is the operator's keyboard:
`Start-XmipTest -Nodes R1, P1, S1` expands `R`, `P` and `S` into the receive,
process and send capability inside the cmdlet, and `-NodeCapability
@{ alpha = 'receive' }` says it outright and overrides the shorthand. What
leaves PowerShell is `XMIP_PLAYGROUND_NODE_CAPABILITIES=R1=receive,…` — a
declaration, not a name.

**Nodes run the test that was named.** The roll passes the scenarios it was
given to every node (`--scenarios`, with every node's name in `--nodes`), and a
node runs its part of those and nothing else: ExclusiveClaim and DailyBacklog
only when they were chosen, or when nothing was, which means all. A name that is
no scenario is REFUSED, by the roll and by a node alike, with exit code 2 and
the scenarios there are; it is never dropped. LowLatency, HeavyLoad, Retention
and Filing stay in the roll's own process, at the cluster's level.

**RoundTrip across nodes is the message path between processes**
(`relay.rs`). The (transport x contract) matrix is split across the nodes that
declared `receive`, a pair's index modulo their count, and a bounded slice of
each share rotates through the rounds so a round still lands in seconds. Per
pair:

- the receiving node lets the Stream arrive through the transport and
  publishes `xmip:///<cluster>/node/<name>/receive/<transport>/<contract>`,
  the identity steps beneath it, then hands what arrived to a node that
  declared `process`;
- that node holds the content contract over it, publishes
  `.../node/<name>/process/<transport>/<contract>`, and hands it to a node
  that declared `send`;
- that node sends it out through the same transport, publishes
  `.../node/<name>/send/<transport>/<contract>` with the identity presentation
  beneath it, and closes the verdict: bytes are counted here.

The node a pair goes to next is a stable hash of the stage and the pair over
the nodes that declared it, so every sender agrees without asking. A node that
declared two stages runs one relay per stage, each with its own inbox. Each
stage takes the same injected faults the schedule does, scaled to the level,
decided by the round the receiving node took the pair in; a stage that fails
hands nothing on. The tally, the standing between a pair's turns and the counts
are the schedule's own (`schedule/ledger.rs`), not a copy. When any node
declares a stage and RoundTrip was chosen the roll does not also run it
in-process; when a stage of the path is declared by nobody the roll is REFUSED
at the start, naming the **capability** that went undeclared, and
`Start-XmipTest` says the same before anything is spawned.

**The handoff** (`handoff.rs`) is a file in the cluster's shared directory, one
inbox per node per stage, `<shared>/handoff/<node>/<stage>/`: written under a
temporary name and renamed into place, claimed by the receiver by rename, so a
file has one holder (the semantics ADR-0024's claim rests on). Every delivered
handoff is a hop, counted per link with the stage at either end and the time of
the last one, published in the node's file and drawn as a link. `--online
false` gates what is outside the cluster only: an offline node takes handoffs
like any other. This rehearses option A of `doc/planning/open-problems.md`
problem 17 in the rig; it rules nothing for the runtime.

**The topology** (`topology.rs`) a roll publishes is the cluster (kind
`cluster`), its nodes (`node`), the stages each runs (`stage`), and under a
receive or a send stage one endpoint per transport it reported on (`endpoint`).
A node's stages are the ones it **declared**, read from the capability record
it publishes, and any it has reported on — never its name. The links are the
handoffs, receive stage to process stage to send stage, for every pair of
stages that exchanged any — pattern `send-receive`, protocol `handoff`, volume
the hops, mood the worst leaf at either end — and the shared store with its
ExclusiveClaim and DailyBacklog links only when those tests ran on a node.

**The run says what it was started with** (`run.rs`): the snapshot carries a
`[run]` table — `cluster`, `tests`, `nodes`, `capabilities`, `online`,
`stress` — that a reader which does not know it skips, and the web GUI shows as
one line on every view. `capabilities` is what each node was started with,
`R1=receive` or `P1=process+send`, a node that declared nothing listed by name
alone.



## Running it

Nothing here starts on its own. Xmip provides its tests as suites, and the
Playground is the first: a person starts a run of it (a roll), a set of
emulated nodes or the web monitor, sees what is running, and stops it — Start,
Get and Stop for each, from the estate's PowerShell module. The commands and
what each one does are the estate's `README.md` (*Beginners*, *Operators* and
*The estate module*), and `Get-Help Start-XmipTest -Full` documents every
parameter; this document does not repeat them. A suite is named
`<Provider>.<Name>` (ADR-0011, ADR-0059), so this one is `Core.Playground`,
which is also the default; a bare `Playground` is refused. A provider's suite
joins by a declaration in `test/suite`, never by an edit to Xmip.

The Playground's tests, by the name a person asks for and the scope segment the
roll publishes under: RoundTrip is `round-trip`, LowLatency is `low-latency`,
HeavyLoad is `heavy-load`, Retention is `retention`, Filing is `filing`,
ExclusiveClaim is `exclusive-claim`, DailyBacklog is `daily-backlog`. `-Test`
tab-completes them, and the estate's Pester files when the suite is
`Core.Estate`. Omit `-Test` and the whole suite runs, and `-Test *` says the
same; `-Test Round*` is RoundTrip. Wildcards, not regular expressions
(ADR-0059), and a pattern that matches nothing is refused before the roll
starts.

Every Start and Stop takes `-WhatIf`. A roll's switches reach it through its
own environment, never yours: `-Stress` is `XMIP_PLAYGROUND_STRESS`
(`calm`, `realistic`, `harsh`, `brutal`), `-Test` is
`XMIP_PLAYGROUND_SCENARIOS` (the scope segments above; unset means all),
`-Nodes` is `XMIP_PLAYGROUND_NODE_NAMES` (the nodes to simulate, by name, one
process each; an empty list is `XMIP_PLAYGROUND_NODES=0`,
no nodes; omitted, the level's own numbered nodes), `-NodeCapability` is
`XMIP_PLAYGROUND_NODE_CAPABILITIES` (what each declares it can do,
`alpha=receive,beta=process+send`; a node it does not name declares nothing —
and this is where the `R`/`P`/`S` shorthand of `-Nodes` has already been
expanded), `-OnlineNodes` is `XMIP_PLAYGROUND_ONLINE_NODES`
(which of them may assume the internet, by name, ADR-0045; unset, every node
reads `XMIP_ONLINE`), `-Duration` is
`XMIP_PLAYGROUND_MAX_SECONDS`, `-TimeFactor` is `XMIP_PLAYGROUND_TIME_FACTOR`
and `-LoadBytes` is `XMIP_PLAYGROUND_LOAD_BYTES`. A roll started by hand —
`cargo run --bin xmip-playground-roll [rounds]` with those variables set — is the same roll,
and `Get-XmipTestStatus` lists it too.

Everything a run writes on this machine goes under `.local-work/playground`
at the repository root: `<cluster>-snapshot.toml`, `<cluster>-history.toml`
and `<cluster>-activity.toml` for the monitors, named for the cluster the roll
was started as (`-Cluster`, required: the owner names the cluster), `roll-<pid>.toml` saying what
each roll was started with, the roll's own lines in `roll-<start time>.log`,
and under `node/` each hand-started node's snapshot and log. The folder is
device-local and ignored by git.
