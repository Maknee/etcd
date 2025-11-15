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
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

func TestReadIndexCache_BasicOperations(t *testing.T) {
	lg := zaptest.NewLogger(t)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)

	// Test cache miss on empty cache
	if _, ok := cache.Get(); ok {
		t.Error("expected cache miss on empty cache")
	}

	// Test cache set and get
	cache.Set(100)
	if index, ok := cache.Get(); !ok || index != 100 {
		t.Errorf("expected cache hit with index 100, got ok=%v index=%d", ok, index)
	}

	// Test cache update
	cache.Set(200)
	if index, ok := cache.Get(); !ok || index != 200 {
		t.Errorf("expected cache hit with index 200, got ok=%v index=%d", ok, index)
	}
}

func TestReadIndexCache_LeaseExpiry(t *testing.T) {
	lg := zaptest.NewLogger(t)
	leaseDuration := 50 * time.Millisecond
	cache := NewReadIndexCache(lg, leaseDuration, true)

	// Set cache value
	cache.Set(100)

	// Verify cache hit immediately
	if _, ok := cache.Get(); !ok {
		t.Error("expected cache hit immediately after set")
	}

	// Wait for lease to expire
	time.Sleep(leaseDuration + 10*time.Millisecond)

	// Verify cache miss after expiry
	if _, ok := cache.Get(); ok {
		t.Error("expected cache miss after lease expiry")
	}
}

func TestReadIndexCache_Invalidation(t *testing.T) {
	lg := zaptest.NewLogger(t)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)

	// Set cache value
	cache.Set(100)

	// Verify cache hit
	if _, ok := cache.Get(); !ok {
		t.Error("expected cache hit before invalidation")
	}

	// Invalidate cache
	cache.Invalidate()

	// Verify cache miss after invalidation
	if _, ok := cache.Get(); ok {
		t.Error("expected cache miss after invalidation")
	}
}

func TestReadIndexCache_DisabledCache(t *testing.T) {
	lg := zaptest.NewLogger(t)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, false)

	// Set cache value
	cache.Set(100)

	// Verify cache miss when disabled
	if _, ok := cache.Get(); ok {
		t.Error("expected cache miss when cache is disabled")
	}
}

func TestReadIndexCache_MonotonicUpdates(t *testing.T) {
	lg := zaptest.NewLogger(t)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)

	// Set initial value
	cache.Set(100)

	// Try to set lower value
	cache.Set(50)

	// Verify cache still has higher value
	if index, ok := cache.Get(); !ok || index != 100 {
		t.Errorf("expected cache to keep higher index 100, got ok=%v index=%d", ok, index)
	}

	// Set higher value
	cache.Set(200)

	// Verify cache updated to higher value
	if index, ok := cache.Get(); !ok || index != 200 {
		t.Errorf("expected cache to update to index 200, got ok=%v index=%d", ok, index)
	}
}

func TestReadIndexCache_ConcurrentAccess(t *testing.T) {
	lg := zaptest.NewLogger(t)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)

	// Run concurrent sets and gets
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(val uint64) {
			for j := 0; j < 100; j++ {
				cache.Set(val)
				cache.Get()
			}
			done <- true
		}(uint64(i * 100))
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify cache is still functional
	cache.Set(1000)
	if index, ok := cache.Get(); !ok || index != 1000 {
		t.Errorf("expected cache to work after concurrent access, got ok=%v index=%d", ok, index)
	}
}

func TestReadIndexCache_MultipleInvalidations(t *testing.T) {
	lg := zaptest.NewLogger(t)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)

	// Set value
	cache.Set(100)

	// Multiple invalidations should be safe
	cache.Invalidate()
	cache.Invalidate()
	cache.Invalidate()

	// Verify cache is still usable after multiple invalidations
	cache.Set(200)
	if index, ok := cache.Get(); !ok || index != 200 {
		t.Errorf("expected cache to work after multiple invalidations, got ok=%v index=%d", ok, index)
	}
}

func TestReadIndexCache_ZeroLeaseDuration(t *testing.T) {
	lg := zaptest.NewLogger(t)
	cache := NewReadIndexCache(lg, 0, true)

	// Set cache value
	cache.Set(100)

	// With zero lease duration, cache should always miss
	if _, ok := cache.Get(); ok {
		t.Error("expected cache miss with zero lease duration")
	}
}

func TestReadIndexCache_StatsTracking(t *testing.T) {
	lg := zaptest.NewLogger(t)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)

	// Initial stats should be zero
	stats := cache.GetStats()
	if stats.Hits != 0 || stats.Misses != 0 {
		t.Errorf("expected zero initial stats, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}

	// Miss on empty cache
	cache.Get()
	stats = cache.GetStats()
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}

	// Set and hit
	cache.Set(100)
	cache.Get()
	stats = cache.GetStats()
	if stats.Hits != 1 || stats.Misses != 1 {
		t.Errorf("expected 1 hit and 1 miss, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}

	// Multiple hits
	cache.Get()
	cache.Get()
	stats = cache.GetStats()
	if stats.Hits != 3 || stats.Misses != 1 {
		t.Errorf("expected 3 hits and 1 miss, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}

	// Invalidate and miss
	cache.Invalidate()
	cache.Get()
	stats = cache.GetStats()
	if stats.Hits != 3 || stats.Misses != 2 {
		t.Errorf("expected 3 hits and 2 misses, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}
}

func BenchmarkReadIndexCache_Get(b *testing.B) {
	lg := zaptest.NewLogger(b)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)
	cache.Set(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get()
	}
}

func BenchmarkReadIndexCache_Set(b *testing.B) {
	lg := zaptest.NewLogger(b)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(uint64(i))
	}
}

func BenchmarkReadIndexCache_Invalidate(b *testing.B) {
	lg := zaptest.NewLogger(b)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)
	cache.Set(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Invalidate()
	}
}

func BenchmarkReadIndexCache_ConcurrentGetSet(b *testing.B) {
	lg := zaptest.NewLogger(b)
	cache := NewReadIndexCache(lg, 100*time.Millisecond, true)

	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			if i%2 == 0 {
				cache.Set(i)
			} else {
				cache.Get()
			}
			i++
		}
	})
}
