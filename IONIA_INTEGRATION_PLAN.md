# IONIA Integration Plan for etcd

**Goal:** Integrate key ideas from the IONIA replication protocol into etcd to improve write latency, read scalability, and overall performance while maintaining etcd's strong consistency guarantees.

**Date:** 2025-11-15
**Status:** Planning Phase

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current etcd Architecture](#2-current-etcd-architecture)
3. [IONIA Key Principles](#3-ionia-key-principles)
4. [Integration Challenges](#4-integration-challenges)
5. [Integration Approaches](#5-integration-approaches)
6. [Phased Implementation Plan](#6-phased-implementation-plan)
7. [Technical Implementation Details](#7-technical-implementation-details)
8. [Performance Analysis](#8-performance-analysis)
9. [Risk Assessment](#9-risk-assessment)
10. [Success Metrics](#10-success-metrics)

---

## 1. Executive Summary

### Objective
Reduce etcd write latency from 2-RTT to 1-RTT and improve read scalability by integrating storage-aware replication concepts from IONIA.

### Key Benefits
- **50% write latency reduction** (2-RTT → 1-RTT)
- **3x read throughput** improvement through follower reads
- **Better SSD utilization** via parallel I/O
- **Maintained strong consistency** (linearizability)

### Timeline
- **Phase 1 (3-6 months):** Parallel replication + ReadIndex optimization
- **Phase 2 (6-9 months):** Version-based follower reads
- **Phase 3 (12+ months):** Optional LSM backend exploration

### Risk Level
**Medium** - Requires careful Raft protocol modifications but builds on proven concepts

---

## 2. Current etcd Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        etcd Server                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌────────────────┐     ┌──────────────────┐              │
│  │  gRPC API      │────>│  KV Server       │              │
│  │  (v3)          │     │  (v3_server.go)  │              │
│  └────────────────┘     └────────┬─────────┘              │
│                                   │                         │
│                    ┌──────────────▼──────────────┐         │
│                    │   Raft Consensus Layer      │         │
│                    │   (etcdserver/raft.go)      │         │
│                    │   - Leader election         │         │
│                    │   - Log replication (2-RTT) │         │
│                    │   - Commit coordination     │         │
│                    └──────────────┬──────────────┘         │
│                                   │                         │
│  ┌────────────────────────────────▼──────────────────────┐ │
│  │            MVCC Layer (mvcc/kvstore.go)              │ │
│  │  - Multiversion concurrency control                 │ │
│  │  - In-memory index (B-tree)                         │ │
│  │  - Revision-based versioning                        │ │
│  └───────────────────────┬──────────────────────────────┘ │
│                          │                                 │
│  ┌───────────────────────▼──────────────────────────────┐ │
│  │     Backend Storage (backend/backend.go)            │ │
│  │  - bbolt (B+tree on disk)                          │ │
│  │  - Batched writes (100ms interval)                 │ │
│  │  - Memory-mapped I/O                               │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │     WAL (storage/wal/)                               │ │
│  │  - Raft log persistence                             │ │
│  │  - Sequential writes                                │ │
│  │  - Durability guarantee                             │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Current Write Path (2-RTT)

```
Client Write Request: PUT(key, value)
│
│ [RTT 1: Client → Leader]
├──> Leader (etcdserver)
│    │
│    ├─> Propose to Raft
│    │   │
│    │   ├─> Append to WAL (wal.Save())
│    │   │
│    │   ├──[Parallel]──> Follower 1
│    │   │                ├─> Append to WAL
│    │   │                └─> Send MsgAppResp
│    │   │
│    │   ├──[Parallel]──> Follower 2
│    │   │                ├─> Append to WAL
│    │   │                └─> Send MsgAppResp
│    │   │
│    │   └─> [Wait for Majority]
│    │
│    │ [RTT 2: Followers → Leader]
│    │
│    ├─> Commit entry (Raft commits)
│    │
│    ├─> Apply to MVCC
│    │   ├─> Update in-memory index
│    │   └─> Write to bbolt (batched)
│    │
│    └─> Return to client
│
└──> Total: 2 RTT

Timeline:
─────────────────────────────────────────────────────
T0: Client sends write
T1: Leader receives, proposes (RTT 1)
T2: Followers append, send ACK (RTT 2 starts)
T3: Leader receives majority, commits, applies
    ACK to client
    Total: ~2 RTT
```

### 2.3 Current Read Path

```
Case 1: Read from Leader
────────────────────────

Client ──[RTT 1]──> Leader
                    ├─> Read from MVCC
                    └─> Return value (linearizable)

Total: 1 RTT


Case 2: Read from Follower (Stale Read - Default)
──────────────────────────────────────────────────

Client ──[RTT 1]──> Follower
                    ├─> Read from local MVCC
                    └─> Return value (potentially stale)

Total: 1 RTT (stale)


Case 3: Read from Follower (Linearizable - Expensive)
──────────────────────────────────────────────────────

Client ──[RTT 1]──> Follower
                    │
                    ├──[RTT 2]──> Leader (get commitIndex)
                    │
                    ├─> Wait for local commitIndex >= leader's
                    │
                    └─> Read and return

Total: 2-3 RTT (expensive!)
```

### 2.4 Key Components

**File Locations:**
- **Raft Layer:** `server/etcdserver/raft.go`, `server/etcdserver/server.go`
- **MVCC Store:** `server/storage/mvcc/kvstore.go`
- **Backend:** `server/storage/backend/backend.go` (bbolt integration)
- **WAL:** `server/storage/wal/`
- **Apply Logic:** `server/etcdserver/apply/`
- **Raft HTTP:** `server/etcdserver/api/rafthttp/`

---

## 3. IONIA Key Principles

### 3.1 Core Ideas from IONIA

1. **Storage-Aware Replication**
   - Exploit storage characteristics (fast memtable, WAL vs slow SSTable)
   - Decouple durability (WAL) from queryability (SSTables)

2. **1-RTT Writes**
   - Parallel replication to all nodes
   - ACK after quorum WAL+Memtable writes
   - Background completion for remaining replicas

3. **Version-Based Read Routing**
   - Track which replicas have which versions
   - Route reads to up-to-date replicas
   - 1-RTT reads from any sufficiently up-to-date node

4. **Background Work Exploitation**
   - LSM compaction happens independently per replica
   - Replicas may have different SSTable structure (same logical data)
   - No blocking on background work

### 3.2 IONIA Write Path (for reference)

```
Client ──[RTT 1]──> Coordinator
                    ║
                    ╠══[Parallel]══> Replica 1 (WAL+Memtable)
                    ║                └─> ACK
                    ╠══[Parallel]══> Replica 2 (WAL+Memtable)
                    ║                └─> ACK (Quorum!)
                    ╚══[Parallel]══> Replica 3 (WAL+Memtable)
                                     └─> ACK

                    [ACK to Client] ← 1 RTT total!

[Background] Ensure all replicas eventually consistent
```

---

## 4. Integration Challenges

### 4.1 Fundamental Differences

```
┌─────────────────────┬──────────────────┬──────────────────┐
│ Aspect              │ etcd (Current)   │ IONIA            │
├─────────────────────┼──────────────────┼──────────────────┤
│ Consensus           │ Raft (required)  │ Custom protocol  │
│ Storage             │ bbolt (B+tree)   │ LSM-tree         │
│ Write Path          │ 2-RTT            │ 1-RTT            │
│ Background Work     │ Limited (batch)  │ Heavy (compact)  │
│ Read Routing        │ Leader-biased    │ Version-aware    │
│ Commit Semantics    │ Raft commit      │ Quorum WAL       │
└─────────────────────┴──────────────────┴──────────────────┘
```

### 4.2 Technical Challenges

1. **Raft Integration**
   - Raft protocol expects 2-phase commit
   - Changing Raft behavior risks correctness
   - Must maintain Raft semantics for cluster management

2. **Storage Backend Mismatch**
   - bbolt is B+tree, not LSM
   - Limited background work to exploit
   - Different I/O characteristics

3. **Consistency Guarantees**
   - etcd provides strict linearizability
   - Kubernetes and other clients depend on this
   - Cannot weaken consistency model

4. **Backward Compatibility**
   - Must not break existing deployments
   - Rolling upgrades required
   - Feature flags for new behavior

5. **Complexity**
   - etcd values simplicity and understandability
   - IONIA adds complexity
   - Must balance performance vs maintainability

---

## 5. Integration Approaches

### 5.1 Approach A: Raft-Compatible IONIA Concepts (RECOMMENDED)

**Strategy:** Integrate IONIA concepts while maintaining Raft protocol

**Key Ideas:**
1. **Parallel Raft Replication** (Keep Raft semantics)
2. **Optimized Follower Reads** (Version tracking)
3. **Storage-Aware Apply** (Decouple WAL from backend)

**Advantages:**
- ✅ Lower risk (Raft protocol unchanged)
- ✅ Backward compatible
- ✅ Incremental adoption
- ✅ Works with existing bbolt

**Disadvantages:**
- ❌ Won't achieve full 1-RTT (Raft still 2-phase)
- ❌ Limited performance gains vs full IONIA

**Estimated Improvement:**
- Write latency: 10-20% reduction (parallel I/O)
- Read throughput: 2-3x (follower reads)

---

### 5.2 Approach B: Raft with Fast-Path Optimization

**Strategy:** Add fast-path 1-RTT for common case, fallback to 2-RTT

**Key Ideas:**
1. **Fast-Path:** Single-round AppendEntries with immediate commit
2. **Slow-Path:** Traditional 2-RTT for conflicts/failures
3. **Conditions:** Leader lease, no concurrent operations

**Advantages:**
- ✅ True 1-RTT in common case
- ✅ Still uses Raft (modified)
- ✅ Graceful degradation

**Disadvantages:**
- ❌ Raft protocol changes (risky)
- ❌ Complexity in dual-path logic
- ❌ Requires careful lease management

**Estimated Improvement:**
- Write latency: 30-50% reduction (when fast-path hits)
- Read throughput: 2-3x (follower reads)

---

### 5.3 Approach C: LSM Backend + Full IONIA Protocol

**Strategy:** Replace bbolt with LSM (RocksDB/Pebble) + implement IONIA-style replication

**Key Ideas:**
1. **New Backend:** Integrate RocksDB or Pebble
2. **IONIA Replication:** Full implementation
3. **Parallel Path:** Keep Raft as option

**Advantages:**
- ✅ Full IONIA benefits (1-RTT, scalable reads)
- ✅ Better write throughput (LSM advantages)
- ✅ Larger dataset support

**Disadvantages:**
- ❌ Major architectural change
- ❌ Years of development
- ❌ Backward compatibility nightmare
- ❌ High risk

**Estimated Improvement:**
- Write latency: 50-66% reduction (1-RTT)
- Write throughput: 2-3x
- Read throughput: 3x+
- Dataset size: 10x+

---

### 5.4 Recommended Hybrid Approach

**Combine elements of A and B for practical, incremental improvement**

```
Phase 1: Raft-Compatible Optimizations (Approach A)
├─> Parallel replication I/O
├─> Version-based follower reads
└─> Storage-aware apply logic

Phase 2: Conditional Fast-Path (Approach B)
├─> Leader lease mechanism
├─> Single-round commit for simple cases
└─> Fallback to traditional Raft

Phase 3: Explore LSM Backend (Approach C - Research)
├─> Prototype integration
├─> Performance testing
└─> Decision point: production or not
```

---

## 6. Phased Implementation Plan

### Phase 1: Foundation & Quick Wins (3-6 months)

**Goal:** Improve performance without changing Raft protocol

#### 1.1 Parallel Replication I/O

**Current State:**
```go
// server/etcdserver/raft.go
func (s *EtcdServer) send(msgs []raftpb.Message) {
    for _, m := range msgs {
        s.r.transport.Send(m)  // Sequential!
    }
}
```

**Improved State:**
```go
// Parallel sending to all followers
func (s *EtcdServer) send(msgs []raftpb.Message) {
    var wg sync.WaitGroup
    for _, m := range msgs {
        wg.Add(1)
        go func(msg raftpb.Message) {
            defer wg.Done()
            s.r.transport.Send(msg)  // Parallel!
        }(m)
    }
    wg.Wait()
}
```

**Impact:**
- Reduced follower replication latency
- Better SSD I/O utilization
- ~10-15% write latency improvement

**Files to Modify:**
- `server/etcdserver/raft.go` (send logic)
- `server/etcdserver/api/rafthttp/` (transport layer)

---

#### 1.2 Optimized ReadIndex Protocol

**Current ReadIndex:**
```
Follower Read Request:
├─[RTT 1]─> Follower → Leader (ReadIndex request)
├─[RTT 2]─> Leader → Followers (heartbeat for leadership)
├─[RTT 3]─> Followers → Leader (ACK)
└─[RTT 4]─> Leader → Follower (ReadIndex response)

Total: 3-4 RTT for linearizable follower read!
```

**Optimized ReadIndex:**
```go
// Cache recent ReadIndex responses with lease
type ReadIndexCache struct {
    mu          sync.RWMutex
    commitIndex uint64
    timestamp   time.Time
    lease       time.Duration  // e.g., 50ms
}

func (s *EtcdServer) ReadIndex(ctx context.Context) (uint64, error) {
    // Fast path: Use cached value if lease valid
    if cached, ok := s.readIndexCache.Get(); ok {
        return cached, nil  // 0 extra RTT!
    }

    // Slow path: Traditional ReadIndex
    return s.raftNode.ReadIndex(ctx)
}
```

**Impact:**
- Follower reads: 1 RTT (vs 3-4 RTT)
- Read throughput: 3x improvement
- No consistency tradeoff (lease-based)

**Files to Modify:**
- `server/etcdserver/v3_server.go` (Range implementation)
- `server/etcdserver/raft.go` (ReadIndex logic)

---

#### 1.3 Version Metadata Tracking

**Goal:** Track which followers have which committed indexes

**Implementation:**
```go
// server/etcdserver/server.go
type FollowerVersionTracker struct {
    mu              sync.RWMutex
    followerIndexes map[uint64]uint64  // follower ID → commit index
    lastUpdate      map[uint64]time.Time
}

func (s *EtcdServer) updateFollowerIndex(followerID, commitIndex uint64) {
    s.versionTracker.mu.Lock()
    defer s.versionTracker.mu.Unlock()
    s.versionTracker.followerIndexes[followerID] = commitIndex
    s.versionTracker.lastUpdate[followerID] = time.Now()
}

func (s *EtcdServer) selectReadReplica(requiredIndex uint64) uint64 {
    // Find follower with commitIndex >= requiredIndex
    s.versionTracker.mu.RLock()
    defer s.versionTracker.mu.RUnlock()

    for id, index := range s.versionTracker.followerIndexes {
        if index >= requiredIndex {
            return id  // Route read here
        }
    }
    return s.Leader()  // Fallback to leader
}
```

**Impact:**
- Smart read routing
- Load balancing across followers
- Foundation for future optimizations

**Files to Modify:**
- `server/etcdserver/server.go` (tracking logic)
- `server/etcdserver/raft.go` (update on MsgAppResp)

---

### Phase 2: Advanced Optimizations (6-12 months)

**Goal:** Reduce write latency closer to 1-RTT

#### 2.1 Leader Lease Mechanism

**Concept:**
```
Leader maintains a lease during which it knows it's the only leader:

┌────────────────────────────────────────────────┐
│ Leader Lease (e.g., 100ms)                     │
│                                                │
│ T0: Heartbeat sent to all followers            │
│ T1: Majority ACKs received                     │
│     → Lease starts for next 100ms              │
│                                                │
│ During lease:                                  │
│   - Leader guaranteed to be leader             │
│   - Can make stronger assumptions              │
│   - Enables fast-path optimizations            │
│                                                │
│ Before lease expires:                          │
│   - Renew with heartbeat                       │
│   - Or fall back to slow path                  │
└────────────────────────────────────────────────┘
```

**Implementation:**
```go
type LeaderLease struct {
    mu         sync.RWMutex
    validUntil time.Time
    term       uint64
}

func (s *EtcdServer) hasValidLease() bool {
    s.lease.mu.RLock()
    defer s.lease.mu.RUnlock()
    return time.Now().Before(s.lease.validUntil) &&
           s.lease.term == s.getTerm()
}

func (s *EtcdServer) renewLease() {
    // After successful heartbeat to majority
    s.lease.mu.Lock()
    defer s.lease.mu.Unlock()
    s.lease.validUntil = time.Now().Add(s.cfg.LeaseTimeout)
    s.lease.term = s.getTerm()
}
```

**Files to Modify:**
- `server/etcdserver/raft.go`
- `server/etcdserver/server.go`

---

#### 2.2 Fast-Path Write (Experimental)

**Concept:** Use single-round commit when safe

**Conditions for Fast-Path:**
1. Leader has valid lease
2. All followers are caught up (< 100 entries behind)
3. No conflicting operations
4. Feature flag enabled

**Write Path:**
```
Fast Path (1-RTT):
──────────────────
Client ──[RTT 1]──> Leader
                    ║
                    ╠══> Follower 1 (AppendEntries + COMMIT)
                    ║              └─> ACK
                    ╠══> Follower 2 (AppendEntries + COMMIT)
                    ║              └─> ACK (Quorum!)
                    ║
                    └──> Client ACK (1 RTT!)

Slow Path (2-RTT) - Fallback:
─────────────────────────────
Traditional Raft when:
- Lease expired
- Followers lagging
- Network issues
```

**Implementation:**
```go
func (s *EtcdServer) processInternalRaftRequestOnce(
    ctx context.Context,
    r pb.InternalRaftRequest,
) (*applyResult, error) {

    // Try fast path
    if s.canUseFastPath() {
        result, err := s.fastPathCommit(ctx, r)
        if err == nil {
            return result, nil
        }
        // Fall through to slow path
    }

    // Slow path: Traditional Raft
    return s.raftRequest(ctx, r)
}

func (s *EtcdServer) canUseFastPath() bool {
    return s.hasValidLease() &&
           s.allFollowersCaughtUp() &&
           s.cfg.FastPathEnabled
}
```

**Risks:**
- Raft protocol modification
- Correctness concerns
- Requires extensive testing

**Mitigation:**
- Feature flag (disabled by default)
- Extensive testing suite
- Formal verification (TLA+)
- Gradual rollout

---

#### 2.3 Deferred Apply Optimization

**Concept:** Decouple Raft commit from MVCC apply

**Current:**
```
Raft Commit ──[Blocking]──> MVCC Apply ──> bbolt Write
                                        (batched, 100ms)

Problem: Commit waits for MVCC processing
```

**Optimized:**
```
Raft Commit ──[Immediate]──> Client ACK ✓
       │
       └──[Async Queue]──> MVCC Apply ──> bbolt Write
                         (happens in background)

Guarantee: Reads see committed data via commit index check
```

**Implementation:**
```go
type DeferredApplier struct {
    queue chan committedEntry
    mu    sync.RWMutex
    // Map of commit index → apply completion
    applied map[uint64]struct{}
}

func (s *EtcdServer) applyCommittedEntries(ents []raftpb.Entry) {
    for _, ent := range ents {
        // Queue for background apply
        s.deferredApplier.queue <- committedEntry{
            index: ent.Index,
            data:  ent.Data,
        }

        // Immediately mark as committable
        s.commitIndex.Store(ent.Index)
    }

    // Don't block on MVCC apply!
}

// Background goroutine
func (s *EtcdServer) runDeferredApplier() {
    for entry := range s.deferredApplier.queue {
        s.applyEntryToMVCC(entry)

        s.deferredApplier.mu.Lock()
        s.deferredApplier.applied[entry.index] = struct{}{}
        s.deferredApplier.mu.Unlock()
    }
}

// Reads wait for apply if needed
func (s *EtcdServer) Range(ctx context.Context, r *pb.RangeRequest) {
    requiredIndex := s.commitIndex.Load()

    // Wait for index to be applied before reading
    s.waitForApply(requiredIndex)

    // Now safe to read
    return s.kv.Range(ctx, r)
}
```

**Impact:**
- Reduced write latency (don't wait for MVCC)
- Better parallelism
- Maintained read consistency

**Files to Modify:**
- `server/etcdserver/apply/`
- `server/etcdserver/server.go`

---

### Phase 3: Long-Term Exploration (12+ months)

**Goal:** Research LSM backend for long-term improvements

#### 3.1 LSM Backend Prototype

**Options:**
1. **RocksDB** (via CGO)
   - Pros: Battle-tested, feature-rich
   - Cons: CGO overhead, complexity

2. **Pebble** (Pure Go)
   - Pros: Go-native, simpler integration
   - Cons: Newer, less battle-tested

3. **Custom LSM**
   - Pros: Optimized for etcd
   - Cons: Years of work

**Recommended:** Start with Pebble prototype

**Integration Points:**
```go
// New interface alongside bbolt
type Backend interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    WriteBatch(batch Batch) error
    NewIterator() Iterator
}

// LSM-specific extensions
type LSMBackend interface {
    Backend
    Flush() error  // Memtable → L0
    Compact(level int) error
    GetMemtableStats() MemtableStats
}
```

**Research Questions:**
1. Performance comparison vs bbolt?
2. Memory usage implications?
3. Crash recovery behavior?
4. Backward compatibility strategy?

---

## 7. Technical Implementation Details

### 7.1 Parallel Replication Implementation

**File:** `server/etcdserver/api/rafthttp/http.go`

**Current Send Logic:**
```go
func (h *streamWriter) run() {
    for {
        select {
        case m := <-h.msgc:
            // Sequential write
            h.send(m)
        }
    }
}
```

**Optimized:**
```go
type parallelSender struct {
    workers   int
    workQueue chan raftpb.Message
    wg        sync.WaitGroup
}

func newParallelSender(workers int) *parallelSender {
    ps := &parallelSender{
        workers:   workers,
        workQueue: make(chan raftpb.Message, 1000),
    }

    // Start worker pool
    for i := 0; i < workers; i++ {
        go ps.worker()
    }

    return ps
}

func (ps *parallelSender) worker() {
    for msg := range ps.workQueue {
        ps.sendMessage(msg)
    }
}

func (h *streamWriter) sendBatch(msgs []raftpb.Message) {
    // Enqueue all messages
    for _, m := range msgs {
        h.parallelSender.workQueue <- m
    }
}
```

---

### 7.2 Version Tracker Implementation

**File:** `server/etcdserver/server.go`

```go
type VersionTracker struct {
    mu sync.RWMutex

    // followerID → last known commit index
    followerCommitIndex map[types.ID]uint64

    // followerID → last update timestamp
    lastHeartbeat map[types.ID]time.Time

    // Stale threshold (follower considered stale if no update)
    staleThreshold time.Duration
}

func NewVersionTracker() *VersionTracker {
    return &VersionTracker{
        followerCommitIndex: make(map[types.ID]uint64),
        lastHeartbeat:       make(map[types.ID]time.Time),
        staleThreshold:      500 * time.Millisecond,
    }
}

// Update follower version based on AppendEntries response
func (vt *VersionTracker) UpdateFollower(id types.ID, matchIndex uint64) {
    vt.mu.Lock()
    defer vt.mu.Unlock()

    vt.followerCommitIndex[id] = matchIndex
    vt.lastHeartbeat[id] = time.Now()
}

// Find best replica for read at given index
func (vt *VersionTracker) SelectReplica(minIndex uint64) (types.ID, bool) {
    vt.mu.RLock()
    defer vt.mu.RUnlock()

    now := time.Now()
    var candidates []types.ID

    for id, commitIdx := range vt.followerCommitIndex {
        // Check if follower is up-to-date and fresh
        if commitIdx >= minIndex &&
           now.Sub(vt.lastHeartbeat[id]) < vt.staleThreshold {
            candidates = append(candidates, id)
        }
    }

    if len(candidates) == 0 {
        return 0, false
    }

    // Load balance: pick random from candidates
    idx := rand.Intn(len(candidates))
    return candidates[idx], true
}

// Get replication lag for follower
func (vt *VersionTracker) GetLag(id types.ID, currentIndex uint64) uint64 {
    vt.mu.RLock()
    defer vt.mu.RUnlock()

    followerIdx, ok := vt.followerCommitIndex[id]
    if !ok {
        return currentIndex  // Unknown = max lag
    }

    if followerIdx >= currentIndex {
        return 0
    }

    return currentIndex - followerIdx
}
```

**Integration:**
```go
// server/etcdserver/raft.go
func (r *raftNode) processMessages(ms []raftpb.Message) {
    for _, m := range ms {
        switch m.Type {
        case raftpb.MsgAppResp:
            // Update version tracker
            r.server.versionTracker.UpdateFollower(
                types.ID(m.From),
                m.Index,  // Match index
            )
        }

        r.raft.Step(m)
    }
}
```

---

### 7.3 Smart Read Routing

**File:** `server/etcdserver/v3_server.go`

```go
func (s *EtcdServer) Range(
    ctx context.Context,
    r *pb.RangeRequest,
) (*pb.RangeResponse, error) {

    // Determine required revision
    var requiredRev int64
    if r.Revision == 0 {
        requiredRev = s.KV().Rev()  // Latest
    } else {
        requiredRev = r.Revision
    }

    // If linearizable and we're a follower, try smart routing
    if r.Serializable == false && !s.isLeader() {
        return s.smartFollowerRead(ctx, r, requiredRev)
    }

    // Default path
    return s.range(ctx, r)
}

func (s *EtcdServer) smartFollowerRead(
    ctx context.Context,
    r *pb.RangeRequest,
    requiredRev int64,
) (*pb.RangeResponse, error) {

    // Convert revision to commit index
    requiredIndex := s.revToCommitIndex(requiredRev)

    // Check if we're up-to-date locally
    localCommitIndex := s.getAppliedIndex()
    if localCommitIndex >= requiredIndex {
        // We have the data! Read locally (1 RTT total)
        return s.range(ctx, r)
    }

    // Check if other followers are up-to-date
    replicaID, found := s.versionTracker.SelectReplica(requiredIndex)
    if found && replicaID != s.ID() {
        // Forward to up-to-date follower
        return s.forwardToReplica(ctx, replicaID, r)
    }

    // Fallback: Use ReadIndex protocol
    if err := s.linearizableReadNotify(ctx); err != nil {
        return nil, err
    }
    return s.range(ctx, r)
}
```

---

## 8. Performance Analysis

### 8.1 Expected Improvements by Phase

```
┌───────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│ Metric    │ Baseline     │ Phase 1      │ Phase 2      │ Phase 3      │
│           │ (Current)    │ (6 months)   │ (12 months)  │ (Future)     │
├───────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│ Write     │ 2 RTT        │ 1.7 RTT      │ 1.2 RTT      │ 1 RTT        │
│ Latency   │ (~10ms)      │ (~8.5ms)     │ (~6ms)       │ (~5ms)       │
│           │              │ -15%         │ -40%         │ -50%         │
├───────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│ Read      │ 1 RTT (L)    │ 1 RTT (L)    │ 1 RTT (L)    │ 1 RTT (L/F)  │
│ Latency   │ 3 RTT (F)    │ 1 RTT (F)    │ 1 RTT (F)    │ 1 RTT (F)    │
│ (Linear)  │              │ -66% (F)     │ -66% (F)     │ -66% (F)     │
├───────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│ Read      │ 10K ops/s    │ 30K ops/s    │ 30K ops/s    │ 50K ops/s    │
│ Throughput│ (leader)     │ (balanced)   │ (balanced)   │ (balanced)   │
│           │              │ +200%        │ +200%        │ +400%        │
├───────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│ Write     │ 5K ops/s     │ 6K ops/s     │ 8K ops/s     │ 15K ops/s    │
│ Throughput│              │ +20%         │ +60%         │ +200%        │
└───────────┴──────────────┴──────────────┴──────────────┴──────────────┘

L = Leader, F = Follower
```

### 8.2 Benchmark Scenarios

**Scenario 1: Kubernetes API Server (Read-Heavy)**
```
Workload: 90% reads, 10% writes
Current: All reads go to leader → bottleneck
Phase 1: Reads distributed across all 3 nodes
Result: 3x read throughput, reduced leader CPU
```

**Scenario 2: Service Discovery (Write-Medium)**
```
Workload: 60% reads, 40% writes
Current: 2-RTT writes, leader-only reads
Phase 2: 1.2-RTT writes, follower reads
Result: 40% write latency reduction, 2x total throughput
```

**Scenario 3: Configuration Store (Low Latency)**
```
Workload: 50% reads, 50% writes, latency-sensitive
Current: 10ms p99 write latency
Phase 2: 6ms p99 write latency
Result: Better user experience, higher QoS
```

---

## 9. Risk Assessment

### 9.1 Technical Risks

```
┌─────────────────────────┬──────────┬────────────┬─────────────┐
│ Risk                    │ Severity │ Likelihood │ Mitigation  │
├─────────────────────────┼──────────┼────────────┼─────────────┤
│ Raft correctness bug    │ CRITICAL │ LOW        │ Extensive   │
│                         │          │            │ testing,    │
│                         │          │            │ TLA+        │
├─────────────────────────┼──────────┼────────────┼─────────────┤
│ Data inconsistency      │ CRITICAL │ LOW        │ Test suite, │
│                         │          │            │ Jepsen      │
├─────────────────────────┼──────────┼────────────┼─────────────┤
│ Performance regression  │ HIGH     │ MEDIUM     │ Benchmarks, │
│                         │          │            │ rollback    │
├─────────────────────────┼──────────┼────────────┼─────────────┤
│ Backward incompatibility│ HIGH     │ MEDIUM     │ Feature     │
│                         │          │            │ flags       │
├─────────────────────────┼──────────┼────────────┼─────────────┤
│ Increased complexity    │ MEDIUM   │ HIGH       │ Docs,       │
│                         │          │            │ simplicity  │
├─────────────────────────┼──────────┼────────────┼─────────────┤
│ Memory overhead         │ MEDIUM   │ MEDIUM     │ Monitoring, │
│                         │          │            │ limits      │
└─────────────────────────┴──────────┴────────────┴─────────────┘
```

### 9.2 Mitigation Strategies

**1. Extensive Testing**
```
Test Suite:
├── Unit tests (existing + new)
├── Integration tests (server/tests/integration)
├── E2E tests (server/tests/e2e)
├── Robustness tests (tests/robustness)
├── Jepsen tests (external)
└── Performance benchmarks (tools/benchmark)
```

**2. Feature Flags**
```go
// server/config/config.go
type ServerConfig struct {
    // ... existing fields ...

    // IONIA-inspired features
    EnableParallelReplication bool
    EnableSmartFollowerReads  bool
    EnableFastPathWrites      bool  // Phase 2+
    EnableLSMBackend          bool  // Phase 3+
}

// Default: All disabled for safety
func DefaultConfig() *ServerConfig {
    return &ServerConfig{
        EnableParallelReplication: false,
        EnableSmartFollowerReads:  false,
        EnableFastPathWrites:      false,
        EnableLSMBackend:          false,
    }
}
```

**3. Gradual Rollout**
```
Rollout Plan:
1. Internal testing (3+ months)
2. Alpha release (opt-in feature flag)
3. Beta with select users
4. General availability (still opt-in)
5. Default enabled (if proven stable)
```

**4. Monitoring & Observability**
```go
// New metrics
var (
    parallelReplicationDuration = prometheus.NewHistogram(...)
    followerReadHitRate        = prometheus.NewGauge(...)
    fastPathSuccessRate        = prometheus.NewGauge(...)
    versionTrackerLag          = prometheus.NewHistogramVec(...)
)
```

---

## 10. Success Metrics

### 10.1 Performance Metrics

**Primary KPIs:**
1. **Write Latency (p50, p99)**
   - Target: -15% (Phase 1), -40% (Phase 2)
2. **Read Throughput**
   - Target: +200% (Phase 1)
3. **Read Latency (Linearizable from Follower)**
   - Target: 1 RTT (from 3 RTT)

**Secondary KPIs:**
1. CPU utilization (leader)
2. Network bandwidth utilization
3. Memory overhead
4. Tail latencies (p99.9)

### 10.2 Reliability Metrics

**Must Maintain:**
1. **Correctness:** 100% (no data loss/corruption)
2. **Availability:** 99.99%+ (no regression)
3. **Recovery Time:** Same or better

### 10.3 Testing Criteria

**Gate for Phase 1 → Phase 2:**
- [ ] All existing tests pass
- [ ] 1000+ hours Jepsen testing (no failures)
- [ ] Performance improvement validated in production-like env
- [ ] No memory leaks in 7-day stress test
- [ ] Backward compatibility verified

**Gate for Phase 2 → Phase 3:**
- [ ] All Phase 1 criteria
- [ ] Fast-path used in >80% of writes (under test workload)
- [ ] No correctness issues in 10,000+ hour testing
- [ ] Proven in production by 10+ early adopters

---

## Appendix A: File Change Checklist

### Phase 1 Changes

```
server/etcdserver/
├── server.go
│   └── Add VersionTracker, parallel send coordination
├── raft.go
│   └── Modify send() for parallel replication
├── v3_server.go
│   └── Add smart follower read logic
└── api/rafthttp/
    ├── http.go
    │   └── Parallel transport implementation
    └── stream.go
        └── Worker pool for sending

server/storage/
└── mvcc/
    └── kvstore.go
        └── Metrics for read routing

tests/
├── integration/
│   └── ionia_integration_test.go (new)
├── e2e/
│   └── ionia_e2e_test.go (new)
└── robustness/
    └── ionia_robustness_test.go (new)
```

### Phase 2 Additional Changes

```
server/etcdserver/
├── server.go
│   ├── Add LeaderLease
│   ├── Add fast-path logic
│   └── Add deferred applier
└── apply/
    ├── applier.go
    │   └── Deferred apply queue
    └── apply_v3.go
        └── Async MVCC updates
```

---

## Appendix B: References

### Papers & Protocols
1. **IONIA Paper:** "IONIA: High-Performance Replication for Modern Disk-based KV Stores" (FAST'24)
2. **Raft Paper:** "In Search of an Understandable Consensus Algorithm" (Diego Ongaro, 2014)
3. **CRAQ Paper:** "Object Storage on CRAQ" (USENIX ATC 2009)
4. **Paxos Made Live:** Google Chubby (PODC 2007)

### Related Work in etcd
- Read Index Optimization: https://github.com/etcd-io/etcd/pull/8514
- Linearizable Read from Follower: https://github.com/etcd-io/etcd/issues/10026
- Backend Performance: https://github.com/etcd-io/etcd/issues/12428

### Testing Resources
- Jepsen etcd tests: https://github.com/jepsen-io/jepsen/tree/main/etcd
- etcd robustness framework: `tests/robustness/`

---

## Next Steps

1. **Review & Feedback** (2 weeks)
   - Share with etcd maintainers
   - Gather community input
   - Refine based on feedback

2. **Prototype Phase 1.1** (1 month)
   - Implement parallel replication
   - Benchmark improvements
   - Validate approach

3. **Go/No-Go Decision** (After prototype)
   - Assess performance gains
   - Evaluate complexity tradeoffs
   - Decide on full implementation

4. **Implementation** (3-6 months for Phase 1)
   - Full development
   - Testing & validation
   - Documentation

---

**Document Version:** 1.0
**Last Updated:** 2025-11-15
**Author:** Planning Committee
**Status:** Draft for Review
