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
	"sync"
	"time"

	"go.uber.org/zap"
)

// ReadIndexCache caches recent ReadIndex responses to reduce latency
// for follower reads. This is inspired by IONIA's approach to reducing
// read latency by avoiding unnecessary round-trips.
//
// The cache uses a lease-based approach: if the cached value is still
// within its lease period, it can be returned immediately (0 RTT overhead).
// Otherwise, a new ReadIndex request must be made (normal Raft path).
type ReadIndexCache struct {
	lg *zap.Logger
	mu sync.RWMutex

	// cachedIndex is the last successfully obtained ReadIndex
	cachedIndex uint64

	// cacheTimestamp is when the cached index was obtained
	cacheTimestamp time.Time

	// leaseDuration is how long a cached value remains valid
	leaseDuration time.Duration

	// enabled indicates whether caching is active
	enabled bool

	// Metrics
	hits   uint64
	misses uint64
}

// NewReadIndexCache creates a new ReadIndex cache
func NewReadIndexCache(lg *zap.Logger, leaseDuration time.Duration, enabled bool) *ReadIndexCache {
	if lg == nil {
		lg = zap.NewNop()
	}
	if leaseDuration == 0 {
		leaseDuration = 50 * time.Millisecond
	}

	return &ReadIndexCache{
		lg:            lg,
		leaseDuration: leaseDuration,
		enabled:       enabled,
	}
}

// Get attempts to retrieve a cached ReadIndex
// Returns the index and true if cache is valid, or 0 and false if cache miss
func (rc *ReadIndexCache) Get() (uint64, bool) {
	if !rc.enabled {
		return 0, false
	}

	rc.mu.RLock()
	defer rc.mu.RUnlock()

	// Check if cache is still valid (within lease)
	if time.Since(rc.cacheTimestamp) <= rc.leaseDuration && rc.cachedIndex > 0 {
		rc.mu.RUnlock()
		rc.mu.Lock()
		rc.hits++
		rc.mu.Unlock()
		rc.mu.RLock()

		if rc.lg.Core().Enabled(zap.DebugLevel) {
			rc.lg.Debug("readindex cache hit",
				zap.Uint64("index", rc.cachedIndex),
				zap.Duration("age", time.Since(rc.cacheTimestamp)),
			)
		}

		return rc.cachedIndex, true
	}

	// Cache miss
	rc.mu.RUnlock()
	rc.mu.Lock()
	rc.misses++
	rc.mu.Unlock()
	rc.mu.RLock()

	return 0, false
}

// Set updates the cache with a new ReadIndex value
func (rc *ReadIndexCache) Set(index uint64) {
	if !rc.enabled {
		return
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.cachedIndex = index
	rc.cacheTimestamp = time.Now()

	if rc.lg.Core().Enabled(zap.DebugLevel) {
		rc.lg.Debug("readindex cache updated",
			zap.Uint64("index", index),
		)
	}
}

// Invalidate clears the cache
// This should be called when leadership changes or term changes
func (rc *ReadIndexCache) Invalidate() {
	if !rc.enabled {
		return
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	oldIndex := rc.cachedIndex
	rc.cachedIndex = 0
	rc.cacheTimestamp = time.Time{}

	if oldIndex > 0 {
		rc.lg.Info("readindex cache invalidated",
			zap.Uint64("old-index", oldIndex),
		)
	}
}

// GetStats returns cache statistics
type ReadIndexCacheStats struct {
	Hits       uint64
	Misses     uint64
	HitRate    float64
	LastUpdate time.Time
	Age        time.Duration
	Valid      bool
}

func (rc *ReadIndexCache) GetStats() ReadIndexCacheStats {
	if !rc.enabled {
		return ReadIndexCacheStats{}
	}

	rc.mu.RLock()
	defer rc.mu.RUnlock()

	stats := ReadIndexCacheStats{
		Hits:       rc.hits,
		Misses:     rc.misses,
		LastUpdate: rc.cacheTimestamp,
	}

	if !rc.cacheTimestamp.IsZero() {
		stats.Age = time.Since(rc.cacheTimestamp)
		stats.Valid = stats.Age <= rc.leaseDuration
	}

	total := stats.Hits + stats.Misses
	if total > 0 {
		stats.HitRate = float64(stats.Hits) / float64(total)
	}

	return stats
}

// Enable enables the cache
func (rc *ReadIndexCache) Enable() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.enabled = true
	rc.lg.Info("readindex cache enabled",
		zap.Duration("lease-duration", rc.leaseDuration),
	)
}

// Disable disables the cache and clears cached data
func (rc *ReadIndexCache) Disable() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.enabled = false
	rc.cachedIndex = 0
	rc.cacheTimestamp = time.Time{}
	rc.lg.Info("readindex cache disabled")
}

// IsEnabled returns whether the cache is enabled
func (rc *ReadIndexCache) IsEnabled() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.enabled
}

// SetLeaseDuration updates the lease duration
// This can be used to tune cache behavior at runtime
func (rc *ReadIndexCache) SetLeaseDuration(d time.Duration) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	old := rc.leaseDuration
	rc.leaseDuration = d
	rc.lg.Info("readindex cache lease duration updated",
		zap.Duration("old", old),
		zap.Duration("new", d),
	)
}
