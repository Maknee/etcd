// Copyright 2025 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdserver

import (
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/types"
)

// VersionTracker tracks the commit index of each follower in the cluster
// to enable intelligent read routing and load balancing.
//
// This is inspired by IONIA's version-aware read routing, where reads can
// be served from any replica that has the required commit index.
type VersionTracker struct {
	lg *zap.Logger
	mu sync.RWMutex

	// followerCommitIndex maps follower ID to its last known commit index
	followerCommitIndex map[types.ID]uint64

	// lastHeartbeat maps follower ID to the timestamp of last update
	lastHeartbeat map[types.ID]time.Time

	// staleThreshold is the duration after which follower info is considered stale
	staleThreshold time.Duration

	// enabled indicates whether version tracking is active
	enabled bool
}

// NewVersionTracker creates a new version tracker
func NewVersionTracker(lg *zap.Logger, staleThreshold time.Duration, enabled bool) *VersionTracker {
	if lg == nil {
		lg = zap.NewNop()
	}
	if staleThreshold == 0 {
		staleThreshold = 500 * time.Millisecond
	}

	return &VersionTracker{
		lg:                  lg,
		followerCommitIndex: make(map[types.ID]uint64),
		lastHeartbeat:       make(map[types.ID]time.Time),
		staleThreshold:      staleThreshold,
		enabled:             enabled,
	}
}

// UpdateFollower updates the commit index for a follower
// This should be called when receiving AppendEntries responses
func (vt *VersionTracker) UpdateFollower(id types.ID, matchIndex uint64) {
	if !vt.enabled {
		return
	}

	vt.mu.Lock()
	defer vt.mu.Unlock()

	oldIndex, exists := vt.followerCommitIndex[id]
	vt.followerCommitIndex[id] = matchIndex
	vt.lastHeartbeat[id] = time.Now()

	if vt.lg.Core().Enabled(zap.DebugLevel) {
		if !exists {
			vt.lg.Debug("tracking new follower",
				zap.String("follower-id", id.String()),
				zap.Uint64("match-index", matchIndex),
			)
		} else if matchIndex > oldIndex {
			vt.lg.Debug("follower index updated",
				zap.String("follower-id", id.String()),
				zap.Uint64("old-index", oldIndex),
				zap.Uint64("new-index", matchIndex),
				zap.Uint64("advancement", matchIndex-oldIndex),
			)
		}
	}
}

// SelectReplica finds the best replica to serve a read request for the given minimum index
// Returns the replica ID and true if a suitable replica is found, or 0 and false otherwise
func (vt *VersionTracker) SelectReplica(minIndex uint64) (types.ID, bool) {
	if !vt.enabled {
		return 0, false
	}

	vt.mu.RLock()
	defer vt.mu.RUnlock()

	now := time.Now()
	var candidates []types.ID

	// Find all replicas that:
	// 1. Have commit index >= required minimum
	// 2. Have fresh information (not stale)
	for id, commitIdx := range vt.followerCommitIndex {
		lastUpdate := vt.lastHeartbeat[id]
		isStale := now.Sub(lastUpdate) > vt.staleThreshold

		if commitIdx >= minIndex && !isStale {
			candidates = append(candidates, id)
		}

		if vt.lg.Core().Enabled(zap.DebugLevel) && commitIdx < minIndex {
			vt.lg.Debug("follower behind required index",
				zap.String("follower-id", id.String()),
				zap.Uint64("follower-index", commitIdx),
				zap.Uint64("required-index", minIndex),
				zap.Uint64("lag", minIndex-commitIdx),
			)
		}
	}

	if len(candidates) == 0 {
		return 0, false
	}

	// Load balance: randomly select from candidates
	// This distributes read load across all up-to-date replicas
	idx := rand.Intn(len(candidates))
	selected := candidates[idx]

	if vt.lg.Core().Enabled(zap.DebugLevel) {
		vt.lg.Debug("selected replica for read",
			zap.String("replica-id", selected.String()),
			zap.Int("candidates-count", len(candidates)),
			zap.Uint64("required-index", minIndex),
		)
	}

	return selected, true
}

// GetLag returns the replication lag (in entries) for a given follower
// Returns 0 if follower is caught up or unknown
func (vt *VersionTracker) GetLag(id types.ID, currentIndex uint64) uint64 {
	if !vt.enabled {
		return 0
	}

	vt.mu.RLock()
	defer vt.mu.RUnlock()

	followerIdx, ok := vt.followerCommitIndex[id]
	if !ok {
		// Unknown follower, assume maximum lag
		return currentIndex
	}

	if followerIdx >= currentIndex {
		return 0
	}

	return currentIndex - followerIdx
}

// GetFollowerIndex returns the commit index of a specific follower
func (vt *VersionTracker) GetFollowerIndex(id types.ID) (uint64, bool) {
	if !vt.enabled {
		return 0, false
	}

	vt.mu.RLock()
	defer vt.mu.RUnlock()

	index, ok := vt.followerCommitIndex[id]
	return index, ok
}

// AllFollowersCaughtUp checks if all followers are within the given lag threshold
// This is useful for determining whether to use fast-path optimizations
func (vt *VersionTracker) AllFollowersCaughtUp(currentIndex uint64, maxLag uint64) bool {
	if !vt.enabled {
		return false
	}

	vt.mu.RLock()
	defer vt.mu.RUnlock()

	if len(vt.followerCommitIndex) == 0 {
		return false
	}

	now := time.Now()
	for id, followerIdx := range vt.followerCommitIndex {
		// Check if information is fresh
		if now.Sub(vt.lastHeartbeat[id]) > vt.staleThreshold {
			return false
		}

		// Check if follower is caught up
		lag := uint64(0)
		if currentIndex > followerIdx {
			lag = currentIndex - followerIdx
		}
		if lag > maxLag {
			return false
		}
	}

	return true
}

// RemoveFollower removes tracking information for a follower
// This should be called when a follower is removed from the cluster
func (vt *VersionTracker) RemoveFollower(id types.ID) {
	if !vt.enabled {
		return
	}

	vt.mu.Lock()
	defer vt.mu.Unlock()

	delete(vt.followerCommitIndex, id)
	delete(vt.lastHeartbeat, id)

	vt.lg.Info("removed follower from version tracker",
		zap.String("follower-id", id.String()),
	)
}

// GetStats returns statistics about follower replication status
type VersionTrackerStats struct {
	FollowerCount    int
	MinIndex         uint64
	MaxIndex         uint64
	AverageLag       float64
	StaleFollowers   int
}

func (vt *VersionTracker) GetStats(currentIndex uint64) VersionTrackerStats {
	if !vt.enabled {
		return VersionTrackerStats{}
	}

	vt.mu.RLock()
	defer vt.mu.RUnlock()

	stats := VersionTrackerStats{
		FollowerCount: len(vt.followerCommitIndex),
		MinIndex:      ^uint64(0), // max uint64
		MaxIndex:      0,
	}

	if stats.FollowerCount == 0 {
		return stats
	}

	now := time.Now()
	var totalLag uint64

	for id, idx := range vt.followerCommitIndex {
		// Update min/max
		if idx < stats.MinIndex {
			stats.MinIndex = idx
		}
		if idx > stats.MaxIndex {
			stats.MaxIndex = idx
		}

		// Calculate lag
		if currentIndex > idx {
			totalLag += (currentIndex - idx)
		}

		// Count stale followers
		if now.Sub(vt.lastHeartbeat[id]) > vt.staleThreshold {
			stats.StaleFollowers++
		}
	}

	stats.AverageLag = float64(totalLag) / float64(stats.FollowerCount)

	return stats
}

// Enable enables version tracking
func (vt *VersionTracker) Enable() {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	vt.enabled = true
	vt.lg.Info("version tracking enabled")
}

// Disable disables version tracking and clears all tracked data
func (vt *VersionTracker) Disable() {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	vt.enabled = false
	vt.followerCommitIndex = make(map[types.ID]uint64)
	vt.lastHeartbeat = make(map[types.ID]time.Time)
	vt.lg.Info("version tracking disabled")
}

// IsEnabled returns whether version tracking is enabled
func (vt *VersionTracker) IsEnabled() bool {
	vt.mu.RLock()
	defer vt.mu.RUnlock()
	return vt.enabled
}
