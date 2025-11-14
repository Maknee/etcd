# Replication Protocols: Read & Write Path Comparison

Comprehensive ASCII diagrams showing read and write paths for major distributed replication protocols.

---

## Table of Contents

1. [Raft](#1-raft)
2. [Multi-Paxos](#2-multi-paxos)
3. [Primary-Backup (Async)](#3-primary-backup-async)
4. [Primary-Backup (Sync/2PC)](#4-primary-backup-sync2pc)
5. [Chain Replication](#5-chain-replication)
6. [CRAQ (Chain Replication with Apportioned Queries)](#6-craq)
7. [Viewstamped Replication (VR)](#7-viewstamped-replication-vr)
8. [EPaxos (Egalitarian Paxos)](#8-epaxos)
9. [IONIA](#9-ionia)
10. [Comparison Summary](#10-comparison-summary)

---

# 1. RAFT

## Overview
Leader-based consensus protocol designed for understandability. All writes go through leader, replicated to followers.

## Write Path

```
Client Write Request: PUT(key, value)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Phase 1: Client to Leader                                   │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> Leader (Node 1)
│             │
│             ├─> Append to local log (uncommitted)
│             ├─> Assign log index & term
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 2: Leader to Followers (AppendEntries)  │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├──[Parallel]──> Follower 2
│             │                │
│             │                ├─> Append to log
│             │                └─> Send ACK
│             │
│             ├──[Parallel]──> Follower 3
│             │                │
│             │                ├─> Append to log
│             │                └─> Send ACK
│             │
│             │ [Wait for Majority ACKs]
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 3: Commit & Apply                       │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├─> Commit entry (mark as committed)
│             ├─> Apply to state machine
│             │
├──[RTT 2]──< ─┘ Reply to Client (success)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Phase 4: Commit Propagation (Next AppendEntries)            │
│ └─────────────────────────────────────────────────────────────┘
│
│             Leader ──> Followers
│             (commitIndex updated in next heartbeat/AppendEntries)
│             │
│             └─> Followers apply committed entries
│
└──> Total Latency: 2 RTT

Timeline:
─────────────────────────────────────────────────────────────────
T0:  Client sends write to Leader
T1:  Leader receives, appends to log, sends AppendEntries (RTT 1)
T2:  Followers receive, append, send ACK (RTT 2 starts)
T3:  Leader receives majority ACKs, commits, replies to client
     Total: ~2 RTT
─────────────────────────────────────────────────────────────────

State Timeline:
┌────────┬──────────┬──────────┬──────────┬──────────┐
│ Time   │ Leader   │ Follower2│ Follower3│ Client   │
├────────┼──────────┼──────────┼──────────┼──────────┤
│ T0     │ -        │ -        │ -        │ Write!   │
│ T1     │ Log[N]   │ -        │ -        │ Waiting  │
│ T2     │ Log[N]   │ Log[N]   │ Log[N]   │ Waiting  │
│ T3     │ COMMIT   │ Log[N]   │ Log[N]   │ ACK!     │
│ T4     │ COMMIT   │ COMMIT   │ COMMIT   │ Done     │
└────────┴──────────┴──────────┴──────────┴──────────┘
```

## Read Path (Linearizable)

```
Case 1: Read from Leader (with ReadIndex)
──────────────────────────────────────────

Client Read Request: GET(key)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Step 1: Client to Leader                                    │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> Leader (Node 1)
│             │
│             ├─> Save readIndex = commitIndex
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Step 2: Confirm Leadership (ReadIndex)        │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├──[Heartbeat]──> Followers (all)
│             │                 │
│             │                 └─> ACK (confirm leader)
│             │
│             │ [Wait for Majority ACKs]
│             │
│             ├─> Apply all entries up to readIndex
│             ├─> Read from state machine
│             │
├──[RTT 2]──< ─┘ Return value to Client
│
└──> Total Latency: 2 RTT (with ReadIndex optimization)

Alternative: Read from Leader (without ReadIndex optimization)
──────────────────────────────────────────────────────────────
If leader is certain of its leadership (recent heartbeat):
│
├──[RTT 1]──> Leader
│             ├─> Read from state machine
│             └─> Return value
│
└──> Total Latency: 1 RTT (but may return stale if leader partitioned)


Case 2: Read from Follower (Stale Read - Default)
──────────────────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Follower (Node 2)
│             │
│             ├─> Read from local state machine
│             │   (may be stale if commits not yet applied)
│             │
│             └─> Return value (potentially stale)
│
└──> Total Latency: 1 RTT (stale read)


Case 3: Read from Follower (Linearizable)
──────────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Follower (Node 2)
│             │
│             ├─> Forward to Leader or ask for commitIndex
│             │
├──[RTT 2]──> Leader
│             ├─> Get current commitIndex
│             └─> Return commitIndex
│             │
├──[RTT 3]──< Follower
│             ├─> Wait for local commitIndex >= leader's
│             ├─> Read from state machine
│             └─> Return value
│
└──> Total Latency: 3 RTT (linearizable follower read)

Summary:
┌─────────────────────┬──────────┬──────────────┐
│ Read Type           │ Latency  │ Consistency  │
├─────────────────────┼──────────┼──────────────┤
│ Leader (optimized)  │ 1 RTT    │ Linearizable*│
│ Leader (ReadIndex)  │ 2 RTT    │ Linearizable │
│ Follower (stale)    │ 1 RTT    │ Stale        │
│ Follower (linear)   │ 3 RTT    │ Linearizable │
└─────────────────────┴──────────┴──────────────┘
* = Risk of stale read during network partition
```

---

# 2. MULTI-PAXOS

## Overview
Classic consensus algorithm. Uses leader (proposer) for liveness, but any node can propose. Phase 1 establishes leadership, Phase 2 replicates values.

## Write Path (Steady State with Established Leader)

```
Client Write Request: PUT(key, value)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Phase 1: Prepare (SKIPPED in steady state)                  │
│ │ Leader already established, skip to Phase 2                 │
│ └─────────────────────────────────────────────────────────────┘
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Phase 2a: Propose                                           │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> Leader/Proposer (Node 1)
│             │
│             ├─> Assign slot number N
│             ├─> Create proposal (N, value)
│             │
│             ├──[Parallel]──> Acceptor 1
│             │                ├─> Accept proposal
│             │                └─> Send Promise
│             │
│             ├──[Parallel]──> Acceptor 2
│             │                ├─> Accept proposal
│             │                └─> Send Promise
│             │
│             ├──[Parallel]──> Acceptor 3
│             │                ├─> Accept proposal
│             │                └─> Send Promise
│             │
│             │ [Wait for Majority Promises]
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 2b: Accept & Learn                      │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├─> Value is chosen (majority accepted)
│             ├─> Notify learners (can be async)
│             │
├──[RTT 2]──< ─┘ Reply to Client (success)
│
└──> Total Latency: 2 RTT (steady state)

Timeline with Leader Election (Cold Start):
─────────────────────────────────────────────
T0:  Client sends write
T1:  Leader sends Prepare(ballot) ──────────> RTT 1
T2:  Acceptors send Promise ────────────────> RTT 2
T3:  Leader sends Accept(N, value) ─────────> RTT 3
T4:  Acceptors send Accepted ───────────────> RTT 4
     Client gets ACK
     Total: ~4 RTT (with leader election)
─────────────────────────────────────────────

Multi-Paxos Optimization (Steady State):
─────────────────────────────────────────
Phase 1 (Prepare/Promise) done once, reused for many instances
Phase 2 (Accept) for each value: 2 RTT
─────────────────────────────────────────

State Timeline (Steady State):
┌────────┬──────────┬──────────┬──────────┬──────────┐
│ Time   │ Proposer │ Accept-1 │ Accept-2 │ Accept-3 │
├────────┼──────────┼──────────┼──────────┼──────────┤
│ T0     │ Accept(N)│ -        │ -        │ -        │
│ T1     │ Waiting  │ Accepted │ Accepted │ Accepted │
│ T2     │ CHOSEN   │ Learned  │ Learned  │ Learned  │
└────────┴──────────┴──────────┴──────────┴──────────┘
```

## Read Path

```
Case 1: Read from Leader/Proposer (Common)
───────────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Leader/Proposer
│             │
│             ├─> Read from local state machine
│             │   (Built from learned values)
│             │
│             └─> Return value
│
└──> Total Latency: 1 RTT

Note: May be stale if leader hasn't learned latest value


Case 2: Read with Paxos Consensus (Strong Consistency)
───────────────────────────────────────────────────────

Client Read Request: GET(key)
│
│ Treat read as a write operation: propose read request
│
├──[RTT 1]──> Proposer
│             │
│             ├─> Propose read operation through Paxos
│             │
│             ├──[Parallel]──> Acceptors (majority)
│             │                └─> Accept read proposal
│             │
├──[RTT 2]──< ─┤
│             ├─> Read chosen, execute read
│             └─> Return value
│
└──> Total Latency: 2 RTT (linearizable)


Case 3: Read from Learner (Stale)
──────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Learner Node
│             │
│             ├─> Read from local learned state
│             │   (May be behind latest consensus)
│             │
│             └─> Return value (potentially stale)
│
└──> Total Latency: 1 RTT (stale read)

Summary:
┌─────────────────────┬──────────┬──────────────┐
│ Read Type           │ Latency  │ Consistency  │
├─────────────────────┼──────────┼──────────────┤
│ From Leader         │ 1 RTT    │ Eventually   │
│ Paxos Read          │ 2 RTT    │ Linearizable │
│ From Learner        │ 1 RTT    │ Stale        │
└─────────────────────┴──────────┴──────────────┘
```

---

# 3. PRIMARY-BACKUP (Async Replication)

## Overview
Simplest replication: primary handles all operations, asynchronously replicates to backups. Fast but can lose data on primary failure.

## Write Path

```
Client Write Request: PUT(key, value)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Step 1: Write to Primary                                    │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> Primary
│             │
│             ├─> Write to local storage
│             ├─> Apply to state machine
│             │
│             ├─> [ASYNC] Replicate to Backup 1 ─┐
│             ├─> [ASYNC] Replicate to Backup 2 ─┤ (fire & forget)
│             ├─> [ASYNC] Replicate to Backup 3 ─┘
│             │
├──[RTT 1]──< ─┴─> ACK to Client immediately!
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Step 2: Backups Apply (Asynchronously)                      │
│ └─────────────────────────────────────────────────────────────┘
│
│             Backup 1 ──> Apply write (eventually)
│             Backup 2 ──> Apply write (eventually)
│             Backup 3 ──> Apply write (eventually)
│
└──> Total Latency: 1 RTT

State Timeline:
┌────────┬──────────┬──────────┬──────────┬──────────┐
│ Time   │ Primary  │ Backup-1 │ Backup-2 │ Client   │
├────────┼──────────┼──────────┼──────────┼──────────┤
│ T0     │ -        │ -        │ -        │ Write!   │
│ T1     │ COMMIT   │ -        │ -        │ ACK!     │
│ T2     │ COMMIT   │ Applying │ -        │ Done     │
│ T3     │ COMMIT   │ COMMIT   │ Applying │ Done     │
│ T4     │ COMMIT   │ COMMIT   │ COMMIT   │ Done     │
└────────┴──────────┴──────────┴──────────┴──────────┘

Replication Lag:
════════════════════════════════════════════════════════
Primary:  [W1]─[W2]─[W3]─[W4]─[W5]  ← Latest
Backup 1: [W1]─[W2]─[W3]            ← Lagging
Backup 2: [W1]─[W2]                 ← More lag
════════════════════════════════════════════════════════

Risk: Data Loss Window
──────────────────────
If primary fails before backups receive data:
   Lost Data = [W4, W5]
```

## Read Path

```
Case 1: Read from Primary (Consistent)
───────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Primary
│             │
│             ├─> Read from local state machine
│             │
│             └─> Return value (always latest)
│
└──> Total Latency: 1 RTT (consistent)


Case 2: Read from Backup (Stale)
─────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Backup
│             │
│             ├─> Read from local state machine
│             │   (Likely stale due to async replication)
│             │
│             └─> Return value (STALE - could be very old)
│
└──> Total Latency: 1 RTT (stale read)

Staleness: Unbounded (depends on replication lag)

Summary:
┌─────────────────────┬──────────┬──────────────┐
│ Read Type           │ Latency  │ Consistency  │
├─────────────────────┼──────────┼──────────────┤
│ From Primary        │ 1 RTT    │ Consistent   │
│ From Backup         │ 1 RTT    │ Stale (lag)  │
└─────────────────────┴──────────┴──────────────┘
```

---

# 4. PRIMARY-BACKUP (Sync/2PC)

## Overview
Synchronous primary-backup using 2-phase commit. Primary coordinates, all backups must ACK before commit. Strong consistency but higher latency.

## Write Path

```
Client Write Request: PUT(key, value)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Phase 1: Prepare (Send to all backups)                      │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> Primary (Coordinator)
│             │
│             ├─> Write to local log (prepared)
│             │
│             ├──[Parallel]──> Backup 1
│             │                ├─> Write to log (prepared)
│             │                └─> Send VOTE-COMMIT
│             │
│             ├──[Parallel]──> Backup 2
│             │                ├─> Write to log (prepared)
│             │                └─> Send VOTE-COMMIT
│             │
│             ├──[Parallel]──> Backup 3
│             │                ├─> Write to log (prepared)
│             │                └─> Send VOTE-COMMIT
│             │
│             │ [Wait for ALL votes]
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 2: Commit (Send decision)               │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├─> Decision: COMMIT (all voted yes)
│             ├─> Commit locally
│             │
│             ├──[Parallel]──> Backup 1 (COMMIT msg)
│             │                └─> Commit
│             │
│             ├──[Parallel]──> Backup 2 (COMMIT msg)
│             │                └─> Commit
│             │
│             ├──[Parallel]──> Backup 3 (COMMIT msg)
│             │                └─> Commit
│             │
├──[RTT 2]──< ─┴─> ACK to Client
│
│ [Optional: Wait for ACKs from backups - RTT 3]
│
└──> Total Latency: 2-3 RTT

State Timeline:
┌────────┬──────────┬──────────┬──────────┬──────────┐
│ Time   │ Primary  │ Backup-1 │ Backup-2 │ Client   │
├────────┼──────────┼──────────┼──────────┼──────────┤
│ T0     │ -        │ -        │ -        │ Write!   │
│ T1     │ PREPARE  │ PREPARE  │ PREPARE  │ Waiting  │
│ T2     │ COMMIT   │ COMMIT   │ COMMIT   │ ACK!     │
└────────┴──────────┴──────────┴──────────┴──────────┘

Blocking Property:
══════════════════════════════════════════════════════
If ANY backup is slow/down → Entire write BLOCKS
Primary: [PREPARE] ──────────────> ⏳ Waiting...
Backup1: [PREPARE] ✓
Backup2: [PREPARE] ✗ (failed/slow) ← Blocks commit!
══════════════════════════════════════════════════════
```

## Read Path

```
Case 1: Read from Primary
─────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Primary
│             │
│             ├─> Read from local committed state
│             │
│             └─> Return value (always latest committed)
│
└──> Total Latency: 1 RTT (linearizable)


Case 2: Read from Backup
─────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Backup
│             │
│             ├─> Read from local committed state
│             │   (Same as primary due to sync replication)
│             │
│             └─> Return value (consistent - same as primary)
│
└──> Total Latency: 1 RTT (linearizable)

Note: Backups have same committed state as primary

Summary:
┌─────────────────────┬──────────┬──────────────┐
│ Read Type           │ Latency  │ Consistency  │
├─────────────────────┼──────────┼──────────────┤
│ From Primary        │ 1 RTT    │ Linearizable │
│ From Backup         │ 1 RTT    │ Linearizable │
└─────────────────────┴──────────┴──────────────┘
```

---

# 5. CHAIN REPLICATION

## Overview
Nodes arranged in a chain. Writes flow from HEAD → tail, reads from TAIL. Provides strong consistency with good read throughput.

## Write Path

```
Client Write Request: PUT(key, value)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Step 1: Write to HEAD                                       │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> HEAD (Node 1)
│             │
│             ├─> Append to local log
│             ├─> Apply to state machine
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Step 2: Propagate through CHAIN               │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├──[RTT 2]──> Middle (Node 2)
│                           │
│                           ├─> Append to local log
│                           ├─> Apply to state machine
│                           │
│                           ├──[RTT 3]──> TAIL (Node 3)
│                                         │
│                                         ├─> Append to local log
│                                         ├─> Apply to state machine
│                                         │
│                           ┌─────────────┘
│                           │
│ ┌─────────────────────────┴───────────────────────────────────┐
│ │ Step 3: ACK from TAIL to Client                             │
│ └─────────────────────────────────────────────────────────────┘
│                           │
├──[RTT 4]──< ───────────────┘ ACK to Client
│
└──> Total Latency: O(N) RTT where N = chain length

For 3-node chain: 4 RTT
For 5-node chain: 6 RTT

Timeline (3-node chain):
─────────────────────────────────────────────────────────────────
T0:  Client sends write
T1:  HEAD receives, processes ─────────────────────> RTT 1
T2:  HEAD → MIDDLE ────────────────────────────────> RTT 2
T3:  MIDDLE → TAIL ────────────────────────────────> RTT 3
T4:  TAIL → Client ACK ────────────────────────────> RTT 4
     Total: 4 RTT (for 3 nodes)
─────────────────────────────────────────────────────────────────

State Timeline:
┌────────┬──────────┬──────────┬──────────┬──────────┐
│ Time   │ HEAD     │ MIDDLE   │ TAIL     │ Client   │
├────────┼──────────┼──────────┼──────────┼──────────┤
│ T0     │ -        │ -        │ -        │ Write!   │
│ T1     │ WRITTEN  │ -        │ -        │ Waiting  │
│ T2     │ WRITTEN  │ WRITTEN  │ -        │ Waiting  │
│ T3     │ WRITTEN  │ WRITTEN  │ WRITTEN  │ Waiting  │
│ T4     │ WRITTEN  │ WRITTEN  │ WRITTEN  │ ACK!     │
└────────┴──────────┴──────────┴──────────┴──────────┘

Chain Visualization:
═══════════════════════════════════════════════════════
Write Flow:
  Client ──┐
           │
        ┌──▼──┐     ┌──────┐     ┌──────┐
        │HEAD │────>│MIDDLE│────>│TAIL  │
        │(N1) │     │ (N2) │     │ (N3) │
        └─────┘     └──────┘     └───┬──┘
                                     │
                                     └──> Client ACK
═══════════════════════════════════════════════════════
```

## Read Path

```
Case 1: Read from TAIL (Standard - Linearizable)
─────────────────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> TAIL (Node 3)
│             │
│             ├─> Read from local state machine
│             │   (Guaranteed to have all committed writes)
│             │
│             └─> Return value
│
└──> Total Latency: 1 RTT (linearizable)

Guarantee: TAIL has all committed data
          (Write not ACK'd until TAIL has it)


Case 2: Read from HEAD or MIDDLE (Non-standard)
────────────────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> HEAD or MIDDLE
│             │
│             ├─> Read from local state machine
│             │   (May have uncommitted data not yet at TAIL)
│             │
│             └─> Return value (UNSAFE - not committed)
│
└──> Total Latency: 1 RTT (dirty read - not linearizable)

Issue: May read data that hasn't reached TAIL yet,
       could be lost if nodes fail.

Summary:
┌─────────────────────┬──────────┬──────────────┐
│ Read Type           │ Latency  │ Consistency  │
├─────────────────────┼──────────┼──────────────┤
│ From TAIL           │ 1 RTT    │ Linearizable │
│ From HEAD/MIDDLE    │ 1 RTT    │ Dirty Read   │
└─────────────────────┴──────────┴──────────────┘

Chain Visualization:
═══════════════════════════════════════════════════════
Read Flow:
        ┌─────┐     ┌──────┐     ┌──────┐
        │HEAD │     │MIDDLE│     │TAIL  │◄─── Client Read
        │(N1) │     │ (N2) │     │ (N3) │
        └─────┘     └──────┘     └───┬──┘
           ✗            ✗             ✓
        (Dirty)      (Dirty)      (Safe)
═══════════════════════════════════════════════════════
```

---

# 6. CRAQ (Chain Replication with Apportioned Queries)

## Overview
Extension of Chain Replication that allows reads from any node (not just TAIL). Uses version numbers to maintain consistency while improving read throughput.

## Write Path

```
Client Write Request: PUT(key, value)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Step 1: Write to HEAD                                       │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> HEAD (Node 1)
│             │
│             ├─> Append to local log (DIRTY version)
│             ├─> Mark as dirty: key_v_dirty = value
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Step 2: Propagate through CHAIN               │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├──[RTT 2]──> Middle (Node 2)
│                           │
│                           ├─> Append to local log (DIRTY)
│                           ├─> Mark as dirty
│                           │
│                           ├──[RTT 3]──> TAIL (Node 3)
│                                         │
│                                         ├─> Append to local log
│                                         ├─> Mark as CLEAN (committed!)
│                                         │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Step 3: Commit ACK backwards through chain    │
│             │ └───────────────────────────────────────────────┘
│                                         │
│                           ┌─────────────┴──> Send ACK upstream
│                           │
│                           ├──[RTT 4]──> Middle (Node 2)
│                           │             │
│                           │             ├─> Mark as CLEAN
│                           │             └─> Send ACK upstream
│                           │
│                           ├──[RTT 5]──> HEAD (Node 1)
│                                         │
│                                         ├─> Mark as CLEAN
│                                         │
│                           ┌─────────────┘
│                           │
├──[RTT 6]──< ───────────────┘ ACK to Client
│
└──> Total Latency: O(2N) RTT where N = chain length

For 3-node chain: 6 RTT (worse than basic Chain!)

Alternative (Optimized): TAIL directly ACKs client
─────────────────────────────────────────────────
HEAD → MIDDLE → TAIL → Client ACK
└──────────────────────> Also send commit backwards
Total: 4 RTT (same as Chain) + background commit propagation

State Timeline:
┌────────┬──────────┬──────────┬──────────┬──────────┐
│ Time   │ HEAD     │ MIDDLE   │ TAIL     │ Client   │
├────────┼──────────┼──────────┼──────────┼──────────┤
│ T0     │ -        │ -        │ -        │ Write!   │
│ T1     │ DIRTY    │ -        │ -        │ Waiting  │
│ T2     │ DIRTY    │ DIRTY    │ -        │ Waiting  │
│ T3     │ DIRTY    │ DIRTY    │ CLEAN    │ ACK!     │
│ T4     │ DIRTY    │ CLEAN    │ CLEAN    │ Done     │
│ T5     │ CLEAN    │ CLEAN    │ CLEAN    │ Done     │
└────────┴──────────┴──────────┴──────────┴──────────┘

Version States:
═══════════════════════════════════════════════════════
Each node maintains versions:
  CLEAN version: Latest committed value
  DIRTY version(s): Propagating writes (not yet committed)

Example:
  HEAD:   key = v2 (DIRTY), v1 (CLEAN)
  MIDDLE: key = v2 (DIRTY), v1 (CLEAN)
  TAIL:   key = v1 (CLEAN)
          (v2 not yet arrived)
═══════════════════════════════════════════════════════
```

## Read Path

```
Case 1: Read from Any Node (CLEAN version - No conflict)
──────────────────────────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Any Node (e.g., Node 2)
│             │
│             ├─> Check local state:
│             │   - key has only CLEAN version
│             │   - No DIRTY versions
│             │
│             ├─> Read CLEAN version from local state
│             │
│             └─> Return value
│
└──> Total Latency: 1 RTT (linearizable)

State:
  Node 2: key = v5 (CLEAN)
  No DIRTY versions → Safe to read locally


Case 2: Read from Node with DIRTY version (Version Query)
──────────────────────────────────────────────────────────

Client Read Request: GET(key)
│
│ Node has DIRTY version (write in progress)
│
├──[RTT 1]──> Node 2 (MIDDLE)
│             │
│             ├─> Check local state:
│             │   - key = v6 (DIRTY)  ← Not yet committed!
│             │   - key = v5 (CLEAN)
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Sub-step: Query TAIL for commit status        │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├──[RTT 2]──> TAIL (Node 3)
│             │             │
│             │             ├─> What's latest CLEAN version?
│             │             │
│             │             └─> Response: v5 is latest CLEAN
│             │                          (v6 not yet committed)
│             │
│             ├─> Return v5 (CLEAN) to client
│             │
├──[RTT 2]──< ─┘
│
└──> Total Latency: 2 RTT (linearizable)

State Before Query:
  MIDDLE: key = v6 (DIRTY), v5 (CLEAN)
  TAIL:   key = v5 (CLEAN)
          (v6 in transit, not yet at TAIL)

Decision: Return v5 (don't return uncommitted v6)


Case 3: Read from TAIL (Always Fast)
─────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> TAIL (Node 3)
│             │
│             ├─> Read from local state
│             │   (TAIL only has CLEAN versions)
│             │
│             └─> Return value
│
└──> Total Latency: 1 RTT (linearizable)

Guarantee: TAIL never has DIRTY versions,
          only committed (CLEAN) data


Summary:
┌─────────────────────┬──────────┬──────────────┐
│ Read Type           │ Latency  │ Consistency  │
├─────────────────────┼──────────┼──────────────┤
│ From any (no dirty) │ 1 RTT    │ Linearizable │
│ From any (w/ dirty) │ 2 RTT    │ Linearizable │
│ From TAIL           │ 1 RTT    │ Linearizable │
└─────────────────────┴──────────┴──────────────┘

CRAQ Advantage:
═══════════════════════════════════════════════════════
Read Load Distribution:
  Chain Replication: All reads → TAIL (bottleneck)
  CRAQ:             Reads → Any Node (balanced)

        ┌─────┐     ┌──────┐     ┌──────┐
Reads ─>│HEAD │  ─> │MIDDLE│  ─> │TAIL  │ <─ Reads
        │(N1) │     │ (N2) │     │ (N3) │
        └─────┘     └──────┘     └──────┘
          ▲           ▲             ▲
          └───Reads───┴─────Reads──┘

Result: 3x read throughput (for 3 nodes)
═══════════════════════════════════════════════════════
```

---

# 7. VIEWSTAMPED REPLICATION (VR)

## Overview
One of the earliest consensus protocols (predates Paxos publication). Similar to Raft with leader-based replication, but different terminology and some protocol details.

## Write Path

```
Client Write Request: PUT(key, value)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Phase 1: Client to Primary                                  │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> Primary (Node 1)
│             │
│             ├─> Assign op-number (monotonic counter)
│             ├─> Append to log (uncommitted)
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 2: Prepare (to Backups)                 │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├──[Parallel]──> Backup 1
│             │                │
│             │                ├─> Append to log
│             │                ├─> Update op-number
│             │                └─> Send PrepareOK
│             │
│             ├──[Parallel]──> Backup 2
│             │                │
│             │                ├─> Append to log
│             │                ├─> Update op-number
│             │                └─> Send PrepareOK
│             │
│             │ [Wait for Majority PrepareOK]
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 3: Commit                               │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├─> Mark as committed (commit-number++)
│             ├─> Execute operation
│             │
├──[RTT 2]──< ─┴─> Reply to Client
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 4: Commit Notification (Next message)   │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├─> Send commit-number to backups
│             │   (piggyback on next Prepare or explicit)
│             │
│             └─> Backups execute operations up to commit-number
│
└──> Total Latency: 2 RTT

Timeline:
─────────────────────────────────────────────────────────────────
T0:  Client sends request to Primary
T1:  Primary receives, sends Prepare to backups ───> RTT 1
T2:  Backups respond with PrepareOK ───────────────> RTT 2
T3:  Primary commits, replies to client
     Total: ~2 RTT
─────────────────────────────────────────────────────────────────

State Timeline:
┌────────┬──────────┬──────────┬──────────┬──────────┐
│ Time   │ Primary  │ Backup-1 │ Backup-2 │ Client   │
├────────┼──────────┼──────────┼──────────┼──────────┤
│ T0     │ -        │ -        │ -        │ Request  │
│ T1     │ Log[N]   │ -        │ -        │ Waiting  │
│ T2     │ Log[N]   │ Log[N]   │ Log[N]   │ Waiting  │
│ T3     │ COMMIT   │ Log[N]   │ Log[N]   │ Reply    │
│ T4     │ COMMIT   │ COMMIT   │ COMMIT   │ Done     │
└────────┴──────────┴──────────┴──────────┴──────────┘

VR vs Raft Terminology:
┌─────────────────┬──────────────┬──────────────┐
│ Concept         │ VR           │ Raft         │
├─────────────────┼──────────────┼──────────────┤
│ Leader          │ Primary      │ Leader       │
│ Followers       │ Backups      │ Followers    │
│ Term            │ View-number  │ Term         │
│ Log index       │ Op-number    │ Log index    │
│ Append          │ Prepare      │ AppendEntry  │
│ Commit index    │ Commit-num   │ CommitIndex  │
└─────────────────┴──────────────┴──────────────┘
```

## Read Path

```
Case 1: Read from Primary (Linearizable)
─────────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Primary
│             │
│             ├─> Read from local state machine
│             │   (Execute all committed operations)
│             │
│             └─> Return value
│
└──> Total Latency: 1 RTT

Note: Primary may need to confirm it's still primary
      (similar to Raft's ReadIndex)


Case 2: Read from Primary (with Lease)
───────────────────────────────────────

If Primary has active lease (knows it's still leader):
│
├──[RTT 1]──> Primary
│             │
│             ├─> Check lease validity
│             ├─> Read from local state machine
│             │
│             └─> Return value
│
└──> Total Latency: 1 RTT (fast path)


Case 3: Read from Backup (Stale)
─────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Backup
│             │
│             ├─> Read from local state machine
│             │   (May be behind primary's commit point)
│             │
│             └─> Return value (potentially stale)
│
└──> Total Latency: 1 RTT (stale read)


Summary:
┌─────────────────────┬──────────┬──────────────┐
│ Read Type           │ Latency  │ Consistency  │
├─────────────────────┼──────────┼──────────────┤
│ From Primary        │ 1 RTT    │ Linearizable │
│ From Primary (lease)│ 1 RTT    │ Linearizable │
│ From Backup         │ 1 RTT    │ Stale        │
└─────────────────────┴──────────┴──────────────┘
```

---

# 8. EPAXOS (Egalitarian Paxos)

## Overview
Leaderless consensus protocol where any node can propose. Optimizes for commutative operations. Complex conflict detection but can achieve 1 RTT for non-conflicting operations.

## Write Path (Fast Path - No Conflicts)

```
Client Write Request: PUT(key, value)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Fast Path: No conflicting operations                        │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> Command Leader (Any node, e.g., Node 1)
│             │
│             ├─> Assign instance ID
│             ├─> Compute dependencies (what commands must run before)
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 1: Pre-Accept (to all other replicas)   │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├──[Parallel]──> Replica 2
│             │                │
│             │                ├─> Check for conflicts
│             │                ├─> Update dependencies
│             │                └─> Send Pre-Accept-OK
│             │
│             ├──[Parallel]──> Replica 3
│             │                │
│             │                ├─> Check for conflicts
│             │                ├─> Update dependencies
│             │                └─> Send Pre-Accept-OK
│             │
│             ├──[Parallel]──> Replica 4, 5...
│             │                └─> Pre-Accept-OK
│             │
│             │ [Wait for Fast Quorum (⌈(N+1)/2⌉ + ⌈(N+1)/4⌉)]
│             │
│             ├─> Check if all dependencies match
│             │
│             │   IF all deps agree: Fast Path! ✓
│             │   ELSE: Slow Path (Accept phase needed)
│             │
│             ├─> Mark as committed
│             │
├──[RTT 1]──< ─┴─> ACK to Client (Fast path!)
│
└──> Total Latency: 1 RTT (fast path, no conflicts)

Fast Quorum for N=5:
  ⌈(5+1)/2⌉ + ⌈(5+1)/4⌉ = 3 + 2 = 5 (all nodes)
  But can commit with 4 in some cases

Timeline (Fast Path):
─────────────────────────────────────────────────────────────────
T0:  Client sends write to Node 1
T1:  Node 1 sends Pre-Accept to all ────────────> RTT 1
T2:  All respond Pre-Accept-OK, deps agree ─────> Client ACK
     Total: 1 RTT ✓ (optimal!)
─────────────────────────────────────────────────────────────────
```

## Write Path (Slow Path - With Conflicts)

```
Client Write Request: PUT(key, value)
│
│ When dependencies don't match or slow quorum only
│
├──[RTT 1]──> Command Leader (Node 1)
│             │
│             ├─> Pre-Accept phase (as before)
│             │
│             │ [Receive Pre-Accept-OK, deps differ] → Slow path
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 2: Accept (with updated deps)           │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├──[Parallel]──> All replicas
│             │                │
│             │                ├─> Accept with final dependencies
│             │                └─> Send Accept-OK
│             │
│             │ [Wait for Simple Majority]
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Phase 3: Commit                               │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├─> Mark as committed
│             │
├──[RTT 2]──< ─┴─> ACK to Client
│
└──> Total Latency: 2 RTT (slow path)

Timeline (Slow Path):
─────────────────────────────────────────────────────────────────
T0:  Client sends write
T1:  Pre-Accept phase, deps don't agree ────────> RTT 1
T2:  Accept phase with final deps ──────────────> RTT 2
T3:  Majority ACK, Client gets response
     Total: 2 RTT
─────────────────────────────────────────────────────────────────

Dependency Example:
═══════════════════════════════════════════════════════
Client 1: PUT(x, 1)  →  Instance I1
Client 2: PUT(x, 2)  →  Instance I2

Conflict detected! Both write to key 'x'

Dependencies:
  I1.deps = {}      (no deps initially)
  I2.deps = {I1}    (must run after I1)

Execution order maintained through dependency graph
═══════════════════════════════════════════════════════
```

## Read Path

```
Case 1: Local Read (Eventual Consistency)
──────────────────────────────────────────

Client Read Request: GET(key)
│
├──[RTT 1]──> Any Replica
│             │
│             ├─> Execute all committed instances affecting key
│             ├─> Follow dependency graph
│             ├─> Read final value
│             │
│             └─> Return value
│
└──> Total Latency: 1 RTT (may be stale)


Case 2: Read with Consensus (Linearizable)
───────────────────────────────────────────

Client Read Request: GET(key)
│
│ Treat read as a write operation (run through EPaxos)
│
├──[RTT 1]──> Command Leader
│             │
│             ├─> Create read command instance
│             ├─> Pre-Accept phase (fast quorum)
│             │
│             │ [If fast path succeeds]
│             │
│             ├─> Execute read after all dependencies
│             │
├──[RTT 1]──< ─┴─> Return value
│
└──> Total Latency: 1 RTT (fast path) or 2 RTT (slow path)


Summary:
┌─────────────────────┬──────────┬──────────────┐
│ Read Type           │ Latency  │ Consistency  │
├─────────────────────┼──────────┼──────────────┤
│ Local read          │ 1 RTT    │ Eventual     │
│ Consensus read (F)  │ 1 RTT    │ Linearizable │
│ Consensus read (S)  │ 2 RTT    │ Linearizable │
└─────────────────────┴──────────┴──────────────┘

EPaxos Advantage:
═══════════════════════════════════════════════════════
Load Distribution:
  Any replica can be command leader
  No single leader bottleneck

        ┌─────┐     ┌─────┐     ┌─────┐
Client→ │ R1  │     │ R2  │     │ R3  │ ←Client
        └──┬──┘     └──┬──┘     └──┬──┘
           └───────────┴───────────┘
         All can propose (leaderless)

Workload: Non-conflicting operations → 1 RTT
          Conflicting operations → 2 RTT
═══════════════════════════════════════════════════════
```

---

# 9. IONIA

## Overview
Storage-aware replication protocol for LSM-tree-based KV stores on SSDs. Exploits storage characteristics for 1-RTT writes and scalable reads.

## Write Path

```
Client Write Request: PUT(key, value)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Step 1: Parallel Replication (Storage-Aware)                │
│ └─────────────────────────────────────────────────────────────┘
│
├──[RTT 1]──> Coordinator (Node 1)
│             │
│             ├─> Write to WAL (fast, sequential)
│             ├─> Write to Memtable (fast, in-memory)
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Sub-step: Parallel to all replicas            │
│             │ └───────────────────────────────────────────────┘
│             │
│             ├──[Parallel]──> Replica 1
│             │                │
│             │                ├─> WAL write (durable)
│             │                ├─> Memtable insert
│             │                └─> Send ACK
│             │
│             ├──[Parallel]──> Replica 2
│             │                │
│             │                ├─> WAL write (durable)
│             │                ├─> Memtable insert
│             │                └─> Send ACK
│             │
│             ├──[Parallel]──> Replica 3
│             │                │
│             │                ├─> WAL write (durable)
│             │                ├─> Memtable insert
│             │                └─> Send ACK
│             │
│             │ [Wait for Quorum ACKs (2/3)]
│             │
│             ├─> Mark as committed (version V assigned)
│             │
├──[RTT 1]──< ─┴─> ACK to Client (1 RTT!)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Background: Async completion & Compaction                   │
│ └─────────────────────────────────────────────────────────────┘
│
│             [Background Process]
│             │
│             ├─> Ensure remaining replicas get write
│             │   (Replica 3 if it was slow)
│             │
│             └─> Each replica independently:
│                 ├─> Flush Memtable → L0 SSTable
│                 ├─> Compact L0 → L1 → L2...
│                 └─> (May differ across replicas!)
│
└──> Total Latency: 1 RTT

Timeline:
─────────────────────────────────────────────────────────────────
T0:  Client sends write
T1:  Coordinator + Replicas write WAL + Memtable ──> RTT 1
     Quorum (2/3) ACKs received → Client ACK
     Total: 1 RTT ✓ (50% faster than Raft!)
─────────────────────────────────────────────────────────────────

State Timeline:
┌────────┬──────────┬──────────┬──────────┬──────────┐
│ Time   │ Coord.   │ Replica-1│ Replica-2│ Client   │
├────────┼──────────┼──────────┼──────────┼──────────┤
│ T0     │ -        │ -        │ -        │ Write!   │
│ T1     │ WAL+MEM  │ WAL+MEM  │ WAL+MEM  │ ACK!✓    │
│ T2     │ v100     │ v100     │ (async)  │ Done     │
│ T3(BG) │ Compact  │ Compact  │ Compact  │ -        │
└────────┴──────────┴──────────┴──────────┴──────────┘

Storage-Aware Optimization:
═══════════════════════════════════════════════════════
Traditional (Raft):
  Wait for: Log append + fsync on majority

IONIA:
  Wait for: WAL + Memtable on quorum
  Defer: SSTable creation, compaction (background)

  ┌────────────────┐
  │   Memtable     │ ← Fast (in-memory)
  ├────────────────┤
  │   WAL          │ ← Fast (sequential write)
  ├────────────────┤
  │   SSTables     │ ← Deferred (background)
  │   L0, L1, L2   │
  └────────────────┘

Insight: Durability ≠ Queryability
  Durable: WAL (fast)
  Queryable: SSTables (can wait)
═══════════════════════════════════════════════════════
```

## Read Path

```
Case 1: Read from Up-to-Date Replica (Common - 1 RTT)
──────────────────────────────────────────────────────

Client Read Request: GET(key)
│
│ ┌─────────────────────────────────────────────────────────────┐
│ │ Step 1: Coordinator checks metadata                         │
│ └─────────────────────────────────────────────────────────────┘
│
├──> Coordinator (or client-side router)
│    │
│    ├─> Metadata: Which replicas have version ≥ V_needed?
│    │
│    │   ┌─────────────────────────────────────────┐
│    │   │ Replica Metadata Cache:                 │
│    │   │  - Replica 1: v100 (up-to-date)         │
│    │   │  - Replica 2: v100 (up-to-date)         │
│    │   │  - Replica 3: v98  (lagging)            │
│    │   │                                         │
│    │   │ Key X last written at: v100             │
│    │   └─────────────────────────────────────────┘
│    │
│    ├─> Select Replica 1 or 2 (both have v100)
│    │
│    │ ┌─────────────────────────────────────────────────────────┐
│    │ │ Step 2: Read from selected replica                      │
│    │ └─────────────────────────────────────────────────────────┘
│    │
├───[RTT 1]──> Replica 1
│              │
│              ├─> Query LSM-tree:
│              │   - Check Memtable first
│              │   - Then SSTables (L0 → L1 → L2...)
│              │
│              └─> Return value
│
├──[RTT 1]──< ─┘
│
└──> Total Latency: 1 RTT (linearizable)


Case 2: Read from Stale Replica (Rare - 2 RTT)
───────────────────────────────────────────────

Client Read Request: GET(key) @ version V100
│
│ Routed to Replica 3 (only has v98)
│
├──[RTT 1]──> Replica 3
│             │
│             ├─> Check local version: v98 < v100 (stale!)
│             │
│             │ ┌───────────────────────────────────────────────┐
│             │ │ Sub-step: Wait or redirect                    │
│             │ └───────────────────────────────────────────────┘
│             │
│             │ Option A: Wait for v100 to arrive (if close)
│             │ Option B: Redirect to up-to-date replica
│             │
│             ├──[RTT 2]──> Replica 1 (redirect)
│             │             │
│             │             └─> Return value @ v100
│             │
├──[RTT 2]──< ─┘
│
└──> Total Latency: 2 RTT (fallback)


Case 3: Read from Any Replica (Version-Aware)
──────────────────────────────────────────────

Read with version requirements:
│
├──[RTT 1]──> Any Replica
│             │
│             ├─> Check: local_version ≥ required_version?
│             │
│             │   YES: Read locally (1 RTT)
│             │   NO:  Redirect or wait (2 RTT)
│             │
│             └─> Return value
│
└──> Total Latency: 1 RTT (most reads) or 2 RTT (rare)

Summary:
┌─────────────────────┬──────────┬──────────────┐
│ Read Type           │ Latency  │ Consistency  │
├─────────────────────┼──────────┼──────────────┤
│ From up-to-date     │ 1 RTT    │ Linearizable │
│ From stale (redir)  │ 2 RTT    │ Linearizable │
│ Local (any replica) │ 1 RTT    │ Eventual     │
└─────────────────────┴──────────┴──────────────┘

IONIA Read Scalability:
═══════════════════════════════════════════════════════
Version Tracking:
  ┌─────────────────────────────────────────┐
  │ Global Version Map (metadata):          │
  │  Key A: v100 (Replicas: 1,2 have it)    │
  │  Key B: v99  (Replicas: 1,2,3 have it)  │
  │  Key C: v101 (Replicas: 1 has it)       │
  └─────────────────────────────────────────┘

Read Distribution:
        Reads ─────> Replica 1 (v101)
        Reads ─────> Replica 2 (v100)
        Reads ─────> Replica 3 (v99)
               (Route based on version needed)

Result: Scalable read throughput without waiting
        for full replication!
═══════════════════════════════════════════════════════
```

---

# 10. COMPARISON SUMMARY

## Write Latency Comparison

```
┌─────────────────────────────────────────────────────────────────┐
│                    Write Latency (RTT)                          │
├─────────────────────┬───────────┬──────────────────────────────┤
│ Protocol            │ Normal    │ Notes                        │
├─────────────────────┼───────────┼──────────────────────────────┤
│ Primary-Backup(Async│ 1 RTT     │ Data loss risk               │
│ Primary-Backup(Sync)│ 2-3 RTT   │ Blocks on all replicas       │
│ Raft                │ 2 RTT     │ Leader + quorum              │
│ Multi-Paxos         │ 2 RTT     │ Steady state                 │
│ Multi-Paxos (cold)  │ 4 RTT     │ With leader election         │
│ VR                  │ 2 RTT     │ Similar to Raft              │
│ Chain (N=3)         │ 4 RTT     │ O(N) latency                 │
│ Chain (N=5)         │ 6 RTT     │ Scales poorly                │
│ CRAQ (N=3)          │ 4-6 RTT   │ Same or worse than Chain     │
│ EPaxos (fast)       │ 1 RTT     │ No conflicts                 │
│ EPaxos (slow)       │ 2 RTT     │ With conflicts               │
│ IONIA               │ 1 RTT     │ Storage-aware ✓              │
└─────────────────────┴───────────┴──────────────────────────────┘

Visual Comparison (3 replicas):
════════════════════════════════════════════════════════════
Raft:
  C ──1──> L ──2──> F ──2──> F
                    ACK ──2──> C
  Total: 2 RTT

Chain:
  C ──1──> H ──2──> M ──3──> T
                         ACK ──4──> C
  Total: 4 RTT

IONIA:
  C ──1──> Coordinator
           ║ (parallel to all)
           ╠══> R1 ──1──> ACK
           ╠══> R2 ──1──> ACK  (quorum!)
           ╚══> R3 ──1──> ACK
           ACK ──1──> C
  Total: 1 RTT ✓
════════════════════════════════════════════════════════════
```

## Read Latency Comparison

```
┌─────────────────────────────────────────────────────────────────┐
│                Read Latency (RTT) - Linearizable                │
├─────────────────────┬───────────┬──────────────────────────────┤
│ Protocol            │ Best Case │ Notes                        │
├─────────────────────┼───────────┼──────────────────────────────┤
│ Primary-Backup      │ 1 RTT     │ Only from primary            │
│ Raft (leader)       │ 1-2 RTT   │ May need ReadIndex           │
│ Raft (follower)     │ 3 RTT     │ Expensive                    │
│ Multi-Paxos         │ 1-2 RTT   │ Or run Paxos for read        │
│ VR                  │ 1 RTT     │ From primary                 │
│ Chain               │ 1 RTT     │ Only from tail               │
│ CRAQ (no dirty)     │ 1 RTT     │ From any replica ✓           │
│ CRAQ (with dirty)   │ 2 RTT     │ Query tail                   │
│ EPaxos              │ 1-2 RTT   │ Run consensus                │
│ IONIA (common)      │ 1 RTT     │ From any up-to-date ✓        │
│ IONIA (rare)        │ 2 RTT     │ Redirect if stale            │
└─────────────────────┴───────────┴──────────────────────────────┘

Read Scalability:
════════════════════════════════════════════════════════════
Raft/Paxos/VR:
  All reads → Leader (bottleneck)
  ┌──────┐
  │Leader│ ←── All client reads
  └──────┘
  Limited by single node capacity

Chain:
  All reads → Tail (bottleneck)
  ┌──────┐
  │ Tail │ ←── All client reads
  └──────┘
  Limited by single node capacity

CRAQ:
  Reads → Any node (distributed)
  ┌────┐   ┌────┐   ┌────┐
  │ N1 │   │ N2 │   │ N3 │
  └─▲──┘   └─▲──┘   └─▲──┘
    └────Reads─────┘
  Scalable read throughput ✓

IONIA:
  Reads → Any up-to-date node (distributed)
  ┌────┐   ┌────┐   ┌────┐
  │ R1 │   │ R2 │   │ R3 │
  └─▲──┘   └─▲──┘   └──┘
    └────Reads──┘  (lagging)
  Version-aware routing ✓
════════════════════════════════════════════════════════════
```

## Protocol Characteristics Matrix

```
┌──────────────┬──────┬──────┬──────┬───────┬──────┬──────┬──────┬──────┐
│              │ P-B  │ P-B  │ Raft │ Paxos │  VR  │ Chain│ CRAQ │IONIA │
│              │(Asyn)│(Sync)│      │       │      │      │      │      │
├──────────────┼──────┼──────┼──────┼───────┼──────┼──────┼──────┼──────┤
│Write Latency │  ✓✓  │  ✗   │  ~   │   ~   │  ~   │  ✗✗  │  ✗✗  │  ✓✓  │
│Read Latency  │  ✓   │  ✓   │  ~   │   ~   │  ✓   │  ✓   │  ✓   │  ✓   │
│Read Scale    │  ✗   │  ✗   │  ✗   │   ✗   │  ✗   │  ✗   │  ✓✓  │  ✓✓  │
│Consistency   │  ✗✗  │  ✓✓  │  ✓✓  │  ✓✓   │  ✓✓  │  ✓✓  │  ✓✓  │  ✓✓  │
│Durability    │  ✗   │  ✓✓  │  ✓✓  │  ✓✓   │  ✓✓  │  ✓✓  │  ✓✓  │  ✓✓  │
│Availability  │  ~   │  ✗   │  ✓   │   ✓   │  ✓   │  ~   │  ~   │  ✓   │
│Simplicity    │  ✓✓  │  ✓✓  │  ✓   │   ✗   │  ✓   │  ✓✓  │  ~   │  ~   │
│Storage-Aware │  ✗   │  ✗   │  ✗   │   ✗   │  ✗   │  ✗   │  ✗   │  ✓✓  │
└──────────────┴──────┴──────┴──────┴───────┴──────┴──────┴──────┴──────┘

Legend: ✓✓ = Excellent, ✓ = Good, ~ = OK, ✗ = Poor, ✗✗ = Very Poor
```

## Use Case Recommendations

```
┌─────────────────────────────────────────────────────────────────┐
│ When to Use Each Protocol                                       │
├─────────────────────┬───────────────────────────────────────────┤
│ Protocol            │ Best For                                  │
├─────────────────────┼───────────────────────────────────────────┤
│ Primary-Backup(Asyn)│ - Low latency required                    │
│                     │ - Can tolerate data loss                  │
│                     │ - Caching/CDN scenarios                   │
├─────────────────────┼───────────────────────────────────────────┤
│ Primary-Backup(Sync)│ - Simple deployment                       │
│                     │ - Strong consistency needed               │
│                     │ - Low throughput OK                       │
├─────────────────────┼───────────────────────────────────────────┤
│ Raft                │ - General-purpose consensus               │
│                     │ - Understandability important             │
│                     │ - Moderate performance needs              │
│                     │ - etcd, Consul, CockroachDB               │
├─────────────────────┼───────────────────────────────────────────┤
│ Multi-Paxos         │ - Production proven (Google Chubby)       │
│                     │ - Need multi-leader flexibility           │
│                     │ - Complex OK for performance              │
├─────────────────────┼───────────────────────────────────────────┤
│ VR                  │ - Similar to Raft                         │
│                     │ - Historical/academic interest            │
├─────────────────────┼───────────────────────────────────────────┤
│ Chain Replication   │ - Read-heavy workloads (99% reads)        │
│                     │ - Can tolerate higher write latency       │
│                     │ - Strong consistency needed               │
│                     │ - Azure Storage uses variant              │
├─────────────────────┼───────────────────────────────────────────┤
│ CRAQ                │ - Read-heavy workloads                    │
│                     │ - Need read scalability                   │
│                     │ - Strong consistency + performance        │
│                     │ - Object storage systems                  │
├─────────────────────┼───────────────────────────────────────────┤
│ EPaxos              │ - Multi-datacenter deployments            │
│                     │ - Low-conflict workloads                  │
│                     │ - Need leaderless operation               │
│                     │ - Geo-distributed systems                 │
├─────────────────────┼───────────────────────────────────────────┤
│ IONIA               │ - LSM-tree based KV stores                │
│                     │ - SSD storage                             │
│                     │ - High write throughput needed            │
│                     │ - Read scalability important              │
│                     │ - RocksDB, LevelDB, Pebble backends       │
└─────────────────────┴───────────────────────────────────────────┘
```

## Combined Protocol Diagram

```
All Protocols Write Path Comparison (3 replicas):
═════════════════════════════════════════════════════════════════

1. Raft (2 RTT):
   Client ──1──> Leader
                 Leader ──║══> Follower 1
                          ║
                          ╚══> Follower 2
                 [Wait quorum] ──2──> Client ACK

2. Chain (4 RTT):
   Client ──1──> Head ──2──> Middle ──3──> Tail ──4──> Client ACK

3. CRAQ (4 RTT + ack propagation):
   Client ──1──> Head ──2──> Middle ──3──> Tail
                                           Tail ──4──> Client ACK
                 [+ack back through chain for clean marking]

4. IONIA (1 RTT):
   Client ──1──> Coordinator
                 Coordinator ══║══> Replica 1
                               ║
                               ╠══> Replica 2
                               ║
                               ╚══> Replica 3
                 [Quorum] ──1──> Client ACK ✓ Fastest!

═════════════════════════════════════════════════════════════════
```

---

## Summary & Recommendations

### Protocol Selection Guide

```
┌──────────────────────────────────────────────────────────────┐
│ Decision Tree                                                │
└──────────────────────────────────────────────────────────────┘

Q: Can you tolerate data loss on failure?
├─ YES → Primary-Backup (Async) [1 RTT writes]
└─ NO → Continue...

Q: Is your storage LSM-tree based (RocksDB/LevelDB)?
├─ YES → Consider IONIA [1 RTT writes, scalable reads]
└─ NO → Continue...

Q: Is workload extremely read-heavy (>90% reads)?
├─ YES → CRAQ or Chain Replication [1 RTT reads from any/tail]
└─ NO → Continue...

Q: Need leaderless/multi-datacenter?
├─ YES → EPaxos [1-2 RTT, geo-distributed]
└─ NO → Continue...

Q: Need proven, understandable consensus?
├─ YES → Raft [2 RTT, widely used]
└─ NO → Multi-Paxos [2 RTT, Google-proven]
```

### Key Insights

1. **No Silver Bullet**: Each protocol optimizes for different scenarios
2. **Latency vs Consistency**: Tradeoffs everywhere
3. **Storage Matters**: IONIA shows storage-aware design wins
4. **Read Scalability**: CRAQ and IONIA allow reads from any replica
5. **Write Latency**: IONIA and EPaxos achieve 1-RTT in best case
6. **Simplicity**: Raft wins for understandability
7. **Production Use**: Raft (etcd, Consul), Paxos (Chubby), Chain (Azure)

