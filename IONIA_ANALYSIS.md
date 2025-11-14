# IONIA: Deep Technical Analysis
## High-Performance Replication for Modern Disk-based KV Stores

**Authors:** Yi Xu*, Henry Zhu*, Prashant Pandey, Alex Conway, Rob Johnson, Aishwarya Ganesan, Ramnatthan Alagappan
(*=co-primary authors)
**Conference:** FAST '24 (22nd USENIX Conference on File and Storage Technologies), February 2024

---

## 1. PROBLEM STATEMENT

### The Core Challenge

Modern write-optimized key-value (WO-KV) stores based on LSM-trees (like RocksDB, LevelDB) have unique performance characteristics that traditional replication protocols (Raft, Paxos, Chain Replication) fail to exploit:

```
Traditional Replication Approach:
┌─────────────────────────────────────────────────────────┐
│  Generic Replication Layer (Raft/Paxos)                │
│  - Treats storage as black box                          │
│  - Synchronous cross-replica coordination               │
│  - All replicas must process writes in lock-step        │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  LSM-based Storage (RocksDB/LevelDB)                    │
│  - Write-optimized with memtable buffering              │
│  - Heavy background compaction work                     │
│  - SSD-optimized I/O patterns                           │
└─────────────────────────────────────────────────────────┘

MISMATCH: Replication layer doesn't leverage storage
          characteristics, leading to suboptimal performance
```

### Key Limitations of Traditional Approaches

1. **Raft/Multi-Paxos Issues:**
   - Requires synchronous replication to quorum (2+ RTTs for commits)
   - Leader handles all writes, limiting scalability
   - Doesn't exploit background processing capabilities of LSM-trees
   - Reads from followers may be stale (need ReadIndex protocol for linearizability)

2. **Chain Replication Issues:**
   - Writes propagate sequentially through chain (high latency)
   - Head node becomes bottleneck
   - All nodes must process write before acknowledgment

3. **Missing Opportunities:**
   - LSM-trees do most work in background (compaction)
   - SSDs have high parallel I/O capabilities
   - Memtable writes are fast (in-memory)
   - Durability comes from WAL, not immediate persistence to SSTables

---

## 2. IONIA'S KEY INNOVATIONS

### Design Principles

**Storage-Aware Replication:** IONIA is designed specifically for LSM-tree-based stores on SSDs, exploiting:
- Fast memtable writes
- Background compaction processes
- SSD parallel I/O characteristics
- Separation of durability (WAL) and queryability (SSTables)

### Three Core Properties (Novel Combination)

1. **1-RTT High-Throughput Writes**
   - Defer parallel execution to background
   - Acknowledge writes quickly while background processes sync replicas

2. **Scalable Reads from Any Replica**
   - Most reads complete in 1-RTT
   - Don't require writes to all replicas first
   - Maintain read consistency without full replication

3. **Write Availability**
   - System remains writable even when some replicas are slow/down
   - No need to block on all replicas

---

## 3. ARCHITECTURE & PROTOCOL DESIGN

### Conceptual Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                         IONIA Layer                            │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  Write Coordination                                      │ │
│  │  - Fast-path: 1-RTT acknowledgment after quorum          │ │
│  │  - Slow-path: Background replication to remaining nodes  │ │
│  └──────────────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  Read Coordination                                       │ │
│  │  - Version tracking across replicas                      │ │
│  │  - Smart replica selection for 1-RTT reads               │ │
│  └──────────────────────────────────────────────────────────┘ │
└────────────────────┬───────────────────────────────┬───────────┘
                     │                               │
┌────────────────────▼────────────┐ ┌───────────────▼────────────┐
│   LSM Storage Exploitation      │ │  SSD Characteristics       │
│   - Leverage memtable speed     │ │  - Parallel I/O            │
│   - Exploit background work     │ │  - High throughput         │
│   - WAL-based durability        │ │  - Low random read latency │
└─────────────────────────────────┘ └────────────────────────────┘
```

### Write Path (Hypothesized based on design goals)

```
Client Write Request: PUT(key, value)
│
├─> [Leader Replica]
│   │
│   ├─> Write to WAL (durability)
│   ├─> Write to Memtable (fast, in-memory)
│   │
│   ├─> [Parallel Replication] ────────────────────┐
│   │   │                                          │
│   │   ├─> Replica 1 ──> WAL + Memtable ─────┐   │
│   │   │                                      │   │
│   │   ├─> Replica 2 ──> WAL + Memtable ─────┤   │
│   │   │                                      │   │
│   │   └─> Replica 3 ──> WAL + Memtable ─────┘   │
│   │                                              │
│   └─> Wait for Quorum (e.g., 2/3) ──────────────┘
│       │
│       └─> ACK to Client (1-RTT!)
│
└─> [Background Process]
    │
    ├─> Ensure all replicas eventually get write
    ├─> Handle stragglers/failures
    └─> Background compaction proceeds independently
        on each replica

Time: O(1 RTT) instead of O(2+ RTT) in traditional Raft
```

**Key Insight:** By leveraging WAL and memtable writes (both fast operations), IONIA can acknowledge to the client after a quorum of replicas have durably logged the write, without waiting for:
- Full replication to all nodes
- Compaction to complete
- SSTables to be written

### Read Path (Hypothesized)

```
Client Read Request: GET(key)
│
├─> [IONIA Read Coordinator]
│   │
│   ├─> Track version/timestamp metadata across replicas
│   │   ┌─────────────────────────────────────────┐
│   │   │ Replica Metadata Cache:                 │
│   │   │  - Replica 1: up-to-date to version V100│
│   │   │  - Replica 2: up-to-date to version V100│
│   │   │  - Replica 3: up-to-date to version V98 │
│   │   └─────────────────────────────────────────┘
│   │
│   ├─> Select appropriate replica(s)
│   │   │
│   │   ├─> CASE 1: Key's last write version ≤ replica's version
│   │   │   └─> Read from single replica (1-RTT) ✓
│   │   │
│   │   └─> CASE 2: Key's last write version > replica's version
│   │       └─> Read from multiple replicas or wait (rare)
│   │
│   └─> Return value to client
│
└─> Result: Most reads complete in 1-RTT

Key Innovation: Don't require all replicas to have all writes!
               Use version tracking + smart routing for consistency
```

### Comparison with Existing Protocols

```
┌─────────────────┬───────────┬──────────────┬─────────────┬──────────┐
│   Property      │   Raft    │ Chain Repl.  │  Paxos-RS   │  IONIA   │
├─────────────────┼───────────┼──────────────┼─────────────┼──────────┤
│ Write Latency   │  2-3 RTT  │   N RTT      │   2-3 RTT   │  1 RTT ✓ │
│ Write Throughput│  Medium   │   Low        │   Medium    │  High  ✓ │
│ Read from Any   │    No     │   Yes (tail) │    No       │  Yes   ✓ │
│ Read Latency    │  1 RTT*   │   1 RTT      │   1 RTT*    │  1 RTT ✓ │
│ Storage-Aware   │    No     │   No         │    No       │  Yes   ✓ │
│ Write Avail.    │  Quorum   │   All nodes  │   Quorum    │ Quorum ✓ │
└─────────────────┴───────────┴──────────────┴─────────────┴──────────┘

* = May require additional protocol round (ReadIndex) for linearizability
N = Number of replicas in chain
```

---

## 4. STORAGE-AWARE OPTIMIZATIONS

### Leveraging LSM-Tree Characteristics

```
Traditional LSM-Tree Write Flow:
┌──────────────────────────────────────────────────────────┐
│ 1. WAL Write          ← Fast, sequential, durable        │
│ 2. Memtable Insert    ← Fast, in-memory                  │
│ 3. [Background] Flush ← Memtable → L0 SSTable            │
│ 4. [Background] Compact ← L0 → L1 → L2 → ... → Ln       │
└──────────────────────────────────────────────────────────┘
                ↑                    ↑
         Fast path (μs)     Slow path (ms-seconds)
        IONIA exploits       Deferred to background
           this!              across replicas

IONIA Insight:
- Replication only needs to ensure WAL + Memtable consistency
- Compaction can happen independently on each replica
- SSTable structure may differ across replicas (same logical data)
- Reads can be served from ANY replica's queryable state
```

### SSD-Specific Optimizations

```
Traditional HDD Constraints:
- Sequential I/O much faster than random
- Limited concurrent operations
- Rotational latency dominates

Modern SSD Capabilities (IONIA exploits):
┌────────────────────────────────────────────┐
│ ✓ High parallel I/O (100K+ IOPS)          │
│ ✓ Low random read latency (~100μs)        │
│ ✓ High sustained throughput (GB/s)        │
│ ✓ No seek time penalty                    │
└────────────────────────────────────────────┘
        │
        ├─> Parallel replication writes
        ├─> Concurrent compaction across replicas
        └─> Fast reads from any replica's SSTables
```

---

## 5. CONSISTENCY MODEL & GUARANTEES

### Consistency Approach (Inferred)

```
┌─────────────────────────────────────────────────────────┐
│  IONIA likely provides:                                 │
│                                                         │
│  ✓ Linearizability for writes (via quorum + ordering)  │
│  ✓ Consistent reads through version tracking           │
│  ✓ Durability via WAL replication to quorum            │
└─────────────────────────────────────────────────────────┘

Version Tracking System:
┌─────────────────────────────────────────────────────────┐
│ Each write gets monotonically increasing version/LSN    │
│                                                         │
│ Write W1 (version 100) ────┐                           │
│                            │                           │
│ ┌─────────┐  ┌─────────┐  ┌▼────────┐                 │
│ │Replica 1│  │Replica 2│  │Replica 3│                 │
│ │ v100 ✓  │  │ v100 ✓  │  │ v98     │ (catching up)  │
│ └─────────┘  └─────────┘  └─────────┘                 │
│                                                         │
│ Read(key) → Coordinator knows:                         │
│   - Last write to key was v100                         │
│   - Replicas 1 & 2 have v100                           │
│   - Can route read to either R1 or R2 (1-RTT)          │
└─────────────────────────────────────────────────────────┘
```

---

## 6. FAILURE HANDLING

### Replica Failure Scenarios

```
Scenario 1: Replica failure during write
┌────────────────────────────────────────────┐
│ Client Write → Replicas [R1, R2, R3]      │
│                                            │
│ R1: ✓ ACK                                  │
│ R2: ✓ ACK  ← Quorum achieved!             │
│ R3: ✗ FAIL                                 │
│                                            │
│ → Write succeeds (1-RTT)                   │
│ → Background process retries R3            │
│ → Eventually R3 catches up                 │
└────────────────────────────────────────────┘

Scenario 2: Stale replica during read
┌────────────────────────────────────────────┐
│ Read(key_X) where last write was v100     │
│                                            │
│ Version state:                             │
│   R1: v100 ✓                               │
│   R2: v95  ✗ (stale)                       │
│   R3: v100 ✓                               │
│                                            │
│ → Coordinator routes to R1 or R3           │
│ → Avoids R2 for this key                  │
│ → Still 1-RTT read!                        │
└────────────────────────────────────────────┘
```

---

## 7. PERFORMANCE BENEFITS

### Theoretical Performance Improvements

```
Write Latency Comparison:
┌──────────────────────────────────────────────────────┐
│                                                      │
│  Raft:                                               │
│  ───────[Leader]───────[RTT]───────[Follower]─────  │
│         [RTT]───────[Follower]                       │
│         [Wait for majority]                          │
│         [RTT]───────[Client ACK]                     │
│  Total: ~2-3 RTT                                     │
│                                                      │
│  IONIA:                                              │
│  ───────[Leader]──────────────┐                     │
│          ║ (parallel)          │                     │
│          ╠══[RTT]══[R1]────────┤                     │
│          ╠══[RTT]══[R2]────────┤ Quorum!             │
│          ╚══[RTT]══[R3]────────┘                     │
│         [Client ACK]                                 │
│  Total: 1 RTT ✓                                      │
│                                                      │
│  Improvement: 50-66% latency reduction               │
└──────────────────────────────────────────────────────┘

Throughput Comparison:
┌──────────────────────────────────────────────────────┐
│  Metric          │  Raft   │  IONIA   │  Improvement │
│  ────────────────┼─────────┼──────────┼───────────   │
│  Write Ops/sec   │  10K    │  25K+    │  2.5x        │
│  Read Ops/sec    │  30K    │  90K+    │  3x          │
│  (Hypothetical numbers based on design goals)        │
└──────────────────────────────────────────────────────┘
```

---

## 8. KEY TECHNICAL CONTRIBUTIONS

### Novel Aspects

1. **Storage-Aware Design**
   - First protocol to deeply integrate with LSM-tree internals
   - Exploits memtable/WAL vs SSTable duality
   - Allows independent background compaction per replica

2. **Decoupled Durability and Queryability**
   - Durability: WAL replication to quorum (fast)
   - Queryability: Eventual SSTable consistency (background)
   - Traditional systems couple these unnecessarily

3. **Version-Based Read Routing**
   - Metadata-driven replica selection
   - Enables 1-RTT reads without sacrificing consistency
   - No need for ReadIndex-like protocols

4. **Parallel Background Sync**
   - Non-quorum replicas catch up asynchronously
   - Doesn't block write path
   - Leverages SSD parallel I/O

---

## 9. RELEVANCE TO ETCD

### Current etcd Architecture

```
etcd Current Design:
┌────────────────────────────────────────┐
│  Raft Consensus Layer                  │
│  - Leader-based replication            │
│  - 2-RTT writes (propose → commit)     │
│  - Follower reads may be stale         │
└──────────────┬─────────────────────────┘
               │
┌──────────────▼─────────────────────────┐
│  bbolt (BoltDB) Backend                │
│  - B+tree structure (not LSM)          │
│  - MVCC layer on top                   │
│  - Copy-on-write pages                 │
│  - mmap-based reads                    │
└────────────────────────────────────────┘

Differences from IONIA's target:
- etcd uses B+tree, not LSM-tree
- etcd optimizes for read consistency (linearizability)
- etcd workloads: coordination, not high-throughput KV
```

### Potential Applications to etcd

#### Direct Application Challenges:
```
┌─────────────────────────────────────────────────────────┐
│ Challenge 1: Storage Backend Mismatch                   │
│  - IONIA designed for LSM-trees (RocksDB/LevelDB)      │
│  - etcd uses bbolt (B+tree)                            │
│  - Would require major storage engine change           │
│                                                         │
│ Challenge 2: Consistency Requirements                   │
│  - etcd provides strict linearizability               │
│  - IONIA's eventual background sync may complicate this│
│  - etcd's use case (coordination) demands strong reads │
│                                                         │
│ Challenge 3: Workload Characteristics                   │
│  - etcd: low-medium throughput, high consistency       │
│  - IONIA optimized for: high throughput, write-heavy   │
└─────────────────────────────────────────────────────────┘
```

#### Lessons & Inspirations:

**1. Storage-Aware Replication Principles:**
```
Even with bbolt, etcd could:
┌────────────────────────────────────────────┐
│ • Optimize Raft for bbolt's characteristics│
│ • Exploit page cache for follower reads    │
│ • Parallelize replication I/O better       │
│ • Reduce fsync coordination overhead       │
└────────────────────────────────────────────┘
```

**2. Read Scalability:**
```
IONIA's read routing → etcd could:
┌────────────────────────────────────────────┐
│ • Better metadata for follower read safety │
│ • Version-based read routing               │
│ • Reduce ReadIndex round-trips             │
│ • Smart replica selection for reads        │
└────────────────────────────────────────────┘
```

**3. Write Path Optimization:**
```
IONIA's 1-RTT writes → etcd could explore:
┌────────────────────────────────────────────┐
│ • Parallel follower replication            │
│ • Earlier acknowledgment after quorum WAL  │
│ • Decouple apply from commit in some cases │
│ • (But must maintain linearizability!)     │
└────────────────────────────────────────────┘
```

**4. If etcd Adopted LSM Backend:**
```
Hypothetical: etcd with RocksDB/Pebble backend
┌──────────────────────────────────────────────┐
│  IONIA-Inspired Replication                  │
│  ├─> 1-RTT writes to quorum (WAL+memtable)  │
│  ├─> Independent compaction per replica     │
│  ├─> Follower reads from local SSTables     │
│  └─> Better write throughput                │
└──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────┐
│  LSM Storage (RocksDB/Pebble)               │
│  ├─> Higher write throughput than bbolt    │
│  ├─> Better SSD utilization                │
│  └─> Suitable for larger datasets          │
└─────────────────────────────────────────────┘

Use cases:
• High-throughput etcd variant
• Large-scale metadata storage
• Time-series workloads
```

---

## 10. KEY CONCLUSIONS

### Main Takeaways

1. **Storage-Awareness Matters:**
   - Generic replication protocols leave performance on the table
   - Deep integration with storage internals enables significant optimizations
   - Modern hardware (SSDs) + modern data structures (LSM) need co-designed protocols

2. **Decoupling is Powerful:**
   - Separate durability (WAL) from queryability (SSTables)
   - Separate fast-path (quorum) from slow-path (full replication)
   - Separate synchronous (critical path) from asynchronous (background)

3. **1-RTT is Achievable:**
   - With careful protocol design
   - By exploiting storage characteristics
   - Without sacrificing consistency or durability

4. **Version Tracking Enables Smart Routing:**
   - Don't need full replication for reads
   - Metadata about replica state allows intelligent decisions
   - Consistency + performance without sacrificing either

### Architectural Patterns to Learn

```
┌─────────────────────────────────────────────────────────┐
│  Pattern 1: Fast-Path / Slow-Path Split                │
│  ─────────────────────────────────────────────────      │
│  Fast: Quorum + WAL → Client ACK (latency-critical)    │
│  Slow: Full replication → Background (throughput)      │
│                                                         │
│  Pattern 2: Metadata-Driven Routing                    │
│  ───────────────────────────────────                    │
│  Maintain version/state metadata about replicas        │
│  Use for smart read/write routing decisions            │
│                                                         │
│  Pattern 3: Storage Co-Design                          │
│  ─────────────────────────────                          │
│  Replication protocol aware of storage internals       │
│  Storage layer exposes hooks for replication           │
│                                                         │
│  Pattern 4: Asynchronous Convergence                   │
│  ────────────────────────────────────                   │
│  Accept partial replication initially                  │
│  Background processes ensure eventual full replication │
└─────────────────────────────────────────────────────────┘
```

### Future Directions

```
Open Questions & Extensions:
├─> How does IONIA handle network partitions?
├─> What are the consistency edge cases?
├─> How does failover/recovery work?
├─> Can this extend to geo-replication?
├─> Application to other storage types (B-trees, etc.)?
└─> Integration with cloud-native architectures?
```

---

## 11. COMPARISON TABLE: IONIA vs TRADITIONAL APPROACHES

```
┌─────────────────────┬────────────┬──────────────┬────────────────┬─────────────┐
│     Aspect          │   Raft     │   Paxos      │  Chain Repl.   │   IONIA     │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ Design Philosophy   │ Generic    │ Generic      │ Generic        │ Storage-    │
│                     │ consensus  │ consensus    │ replication    │ aware       │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ Storage Integration │ None       │ None         │ None           │ Deep (LSM)  │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ Write Latency       │ 2-3 RTT    │ 2+ RTT       │ O(N) RTT       │ 1 RTT       │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ Write Throughput    │ Leader-    │ Moderate     │ Chain-bound    │ High        │
│                     │ bound      │              │                │ (parallel)  │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ Read Scalability    │ Limited*   │ Limited*     │ Good (tail)    │ Excellent   │
│                     │            │              │                │ (any node)  │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ Read Latency        │ 1 RTT*     │ 1 RTT*       │ 1 RTT          │ 1 RTT       │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ SSD Optimization    │ No         │ No           │ No             │ Yes         │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ Background Work     │ Ignored    │ Ignored      │ Ignored        │ Exploited   │
│ Exploitation        │            │              │                │             │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ Partial Replication │ No (quorum │ No (quorum   │ No (all in     │ Yes         │
│ for Reads           │ for write, │ for commits) │ chain)         │ (version-   │
│                     │ stale ok)  │              │                │ tracked)    │
├─────────────────────┼────────────┼──────────────┼────────────────┼─────────────┤
│ Complexity          │ Moderate   │ High         │ Low            │ Moderate-   │
│                     │            │              │                │ High        │
└─────────────────────┴────────────┴──────────────┴────────────────┴─────────────┘

* = May require additional round-trip for linearizable reads (ReadIndex in Raft)
N = Number of replicas in chain
```

---

## 12. IMPLEMENTATION CONSIDERATIONS

### For Systems Considering IONIA-like Approaches

```
┌─────────────────────────────────────────────────────────────┐
│  Requirements Checklist:                                    │
│  ─────────────────────────                                  │
│  ☑ LSM-tree based storage engine (RocksDB, LevelDB, Pebble)│
│  ☑ SSD-backed storage (to exploit parallel I/O)            │
│  ☑ High write throughput requirements                      │
│  ☑ Can tolerate moderate protocol complexity               │
│  ☑ Need read scalability across replicas                   │
│  ☑ Willing to maintain version metadata                    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Anti-patterns (where IONIA may not fit):                   │
│  ────────────────────────────────────────                   │
│  ✗ B-tree or other non-LSM storage                          │
│  ✗ HDD-backed systems (different I/O characteristics)      │
│  ✗ Low-latency coordination workloads (etcd-like)          │
│  ✗ Systems requiring strict ACID across operations         │
│  ✗ Very small datasets (overhead not worth it)             │
└─────────────────────────────────────────────────────────────┘
```

---

## 13. ASCII DIAGRAMS SUMMARY

### System View

```
                    ┌─────────────────────┐
                    │  IONIA Design Goal  │
                    │  ─────────────────  │
                    │  • 1-RTT writes     │
                    │  • 1-RTT reads      │
                    │  • High throughput  │
                    │  • Scalable reads   │
                    └──────────┬──────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
    ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
    │  Exploit    │    │  Leverage   │    │  Optimize   │
    │  Storage    │    │  SSD        │    │  for        │
    │  (LSM)      │    │  Parallel   │    │  Modern     │
    │             │    │  I/O        │    │  Workloads  │
    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
           │                   │                   │
           └───────────────────┴───────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Storage-Aware      │
                    │  Replication        │
                    │  Protocol           │
                    └─────────────────────┘
```

### Protocol Flow Summary

```
WRITE PATH:                          READ PATH:
─────────────                        ──────────
Client                               Client
  │                                    │
  ├─> [Coordinator]                    ├─> [Coordinator]
  │    │                               │    │
  │    ├─> WAL write (parallel)        │    ├─> Check metadata
  │    ├─> Memtable insert             │    │   (which replicas
  │    │                               │    │    have version?)
  │    ├─> Wait quorum (2/3)           │    │
  │    │                               │    ├─> Route to
  │    └─> ACK ← 1 RTT! ✓              │    │   up-to-date replica
  │                                    │    │
  │    [Background]                    │    └─> Read ← 1 RTT! ✓
  │    └─> Sync remaining              │
  │        replicas                    │
  │                                    │
```

---

## REFERENCES & FURTHER READING

- **Paper:** "IONIA: High-Performance Replication for Modern Disk-based KV Stores"
  - Yi Xu*, Henry Zhu*, et al.
  - FAST '24, February 2024, Pages 225-241
  - https://www.usenix.org/conference/fast24/presentation/xu

- **Related Work:**
  - Raft Consensus: https://raft.github.io/
  - Chain Replication: van Renesse & Schneider, OSDI 2004
  - LSM-Trees: O'Neil et al., Acta Informatica 1996
  - RocksDB: https://rocksdb.org/

- **UIUC DASSL Lab:** https://dassl-uiuc.github.io/

---

*This analysis is based on publicly available information about IONIA and deep knowledge of distributed systems, storage engines, and replication protocols. Some implementation details are inferred from design goals and principles.*
