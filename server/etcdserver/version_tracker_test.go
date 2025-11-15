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

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.uber.org/zap/zaptest"
)

func TestVersionTracker_BasicOperations(t *testing.T) {
	lg := zaptest.NewLogger(t)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// Test initial state - no followers tracked
	lag := vt.GetLag(types.ID(1), 100)
	if lag != 100 {
		t.Errorf("expected full lag for untracked follower, got %d", lag)
	}

	// Update follower 1 to index 50
	vt.UpdateFollower(types.ID(1), 50)

	// Check lag for follower 1
	lag = vt.GetLag(types.ID(1), 100)
	if lag != 50 {
		t.Errorf("expected lag of 50, got %d", lag)
	}

	// Update follower 1 to index 100
	vt.UpdateFollower(types.ID(1), 100)
	lag = vt.GetLag(types.ID(1), 100)
	if lag != 0 {
		t.Errorf("expected lag of 0, got %d", lag)
	}
}

func TestVersionTracker_SelectReplica(t *testing.T) {
	lg := zaptest.NewLogger(t)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// No followers tracked - should return false
	_, ok := vt.SelectReplica(100)
	if ok {
		t.Error("expected no replica selected when none tracked")
	}

	// Add follower 1 at index 50
	vt.UpdateFollower(types.ID(1), 50)

	// Request index 100 - follower 1 is behind
	_, ok = vt.SelectReplica(100)
	if ok {
		t.Error("expected no replica selected when all are behind")
	}

	// Request index 50 - follower 1 is caught up
	id, ok := vt.SelectReplica(50)
	if !ok || id != types.ID(1) {
		t.Errorf("expected follower 1 to be selected, got ok=%v id=%d", ok, id)
	}

	// Add follower 2 at index 100
	vt.UpdateFollower(types.ID(2), 100)

	// Request index 100 - follower 2 should be selected
	id, ok = vt.SelectReplica(100)
	if !ok {
		t.Error("expected a replica to be selected")
	}
	if id != types.ID(1) && id != types.ID(2) {
		t.Errorf("expected follower 1 or 2, got %d", id)
	}
}

func TestVersionTracker_MultipleFollowers(t *testing.T) {
	lg := zaptest.NewLogger(t)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// Add 5 followers at different indexes
	vt.UpdateFollower(types.ID(1), 100)
	vt.UpdateFollower(types.ID(2), 200)
	vt.UpdateFollower(types.ID(3), 150)
	vt.UpdateFollower(types.ID(4), 180)
	vt.UpdateFollower(types.ID(5), 220)

	// Request index 150 - should select from followers 2, 4, or 5
	id, ok := vt.SelectReplica(150)
	if !ok {
		t.Error("expected a replica to be selected")
	}
	if id != types.ID(2) && id != types.ID(4) && id != types.ID(5) {
		t.Errorf("expected follower 2, 4, or 5, got %d", id)
	}

	// Verify lags
	testCases := []struct {
		followerID   types.ID
		currentIndex uint64
		expectedLag  uint64
	}{
		{types.ID(1), 250, 150},
		{types.ID(2), 250, 50},
		{types.ID(3), 250, 100},
		{types.ID(4), 250, 70},
		{types.ID(5), 250, 30},
	}

	for _, tc := range testCases {
		lag := vt.GetLag(tc.followerID, tc.currentIndex)
		if lag != tc.expectedLag {
			t.Errorf("follower %d: expected lag %d, got %d",
				tc.followerID, tc.expectedLag, lag)
		}
	}
}

func TestVersionTracker_StaleFollowers(t *testing.T) {
	lg := zaptest.NewLogger(t)
	staleThreshold := 100 * time.Millisecond
	vt := NewVersionTracker(lg, staleThreshold, true)

	// Add follower 1
	vt.UpdateFollower(types.ID(1), 100)

	// Should be selected immediately
	_, ok := vt.SelectReplica(100)
	if !ok {
		t.Error("expected follower to be selected immediately")
	}

	// Wait for follower to become stale
	time.Sleep(staleThreshold + 50*time.Millisecond)

	// Should not be selected when stale
	_, ok = vt.SelectReplica(100)
	if ok {
		t.Error("expected stale follower not to be selected")
	}

	// Update follower 1 again (refresh heartbeat)
	vt.UpdateFollower(types.ID(1), 100)

	// Should be selected again
	_, ok = vt.SelectReplica(100)
	if !ok {
		t.Error("expected follower to be selected after refresh")
	}
}

func TestVersionTracker_AllFollowersCaughtUp(t *testing.T) {
	lg := zaptest.NewLogger(t)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// No followers - should return false
	if vt.AllFollowersCaughtUp(100, 10) {
		t.Error("expected false when no followers tracked")
	}

	// Add follower 1 at index 90, current is 100, maxLag is 10
	vt.UpdateFollower(types.ID(1), 90)
	if !vt.AllFollowersCaughtUp(100, 10) {
		t.Error("expected true when follower within maxLag")
	}

	// Add follower 2 at index 80 (lagging by 20)
	vt.UpdateFollower(types.ID(2), 80)
	if vt.AllFollowersCaughtUp(100, 10) {
		t.Error("expected false when one follower exceeds maxLag")
	}

	// Update follower 2 to 95
	vt.UpdateFollower(types.ID(2), 95)
	if !vt.AllFollowersCaughtUp(100, 10) {
		t.Error("expected true when all followers within maxLag")
	}
}

func TestVersionTracker_Disabled(t *testing.T) {
	lg := zaptest.NewLogger(t)
	vt := NewVersionTracker(lg, 1*time.Second, false)

	// Update follower
	vt.UpdateFollower(types.ID(1), 100)

	// Should not select replica when disabled
	_, ok := vt.SelectReplica(100)
	if ok {
		t.Error("expected no replica selected when disabled")
	}

	// GetLag should still work
	lag := vt.GetLag(types.ID(1), 100)
	if lag != 100 {
		t.Errorf("expected full lag when disabled, got %d", lag)
	}
}

func TestVersionTracker_ConcurrentUpdates(t *testing.T) {
	lg := zaptest.NewLogger(t)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// Run concurrent updates
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(followerID int) {
			for j := 0; j < 100; j++ {
				vt.UpdateFollower(types.ID(followerID), uint64(j))
				vt.GetLag(types.ID(followerID), uint64(j+10))
				vt.SelectReplica(uint64(j))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify tracker is still functional
	vt.UpdateFollower(types.ID(999), 1000)
	lag := vt.GetLag(types.ID(999), 1100)
	if lag != 100 {
		t.Errorf("expected lag of 100, got %d", lag)
	}
}

func TestVersionTracker_LoadBalancing(t *testing.T) {
	lg := zaptest.NewLogger(t)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// Add 3 followers all at index 100
	vt.UpdateFollower(types.ID(1), 100)
	vt.UpdateFollower(types.ID(2), 100)
	vt.UpdateFollower(types.ID(3), 100)

	// Select replica multiple times and track distribution
	selections := make(map[types.ID]int)
	for i := 0; i < 300; i++ {
		id, ok := vt.SelectReplica(100)
		if !ok {
			t.Fatal("expected replica to be selected")
		}
		selections[id]++
	}

	// Verify all followers were selected at least once (basic load balancing)
	if len(selections) != 3 {
		t.Errorf("expected all 3 followers to be selected, got %d", len(selections))
	}

	// Each should be selected roughly equally (within reasonable variance)
	for id, count := range selections {
		if count < 50 || count > 150 {
			t.Errorf("follower %d selected %d times, expected roughly 100", id, count)
		}
	}
}

func TestVersionTracker_GetStats(t *testing.T) {
	lg := zaptest.NewLogger(t)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// Add some followers
	vt.UpdateFollower(types.ID(1), 100)
	vt.UpdateFollower(types.ID(2), 200)
	vt.UpdateFollower(types.ID(3), 150)

	// Get stats with current index 250
	stats := vt.GetStats(250)

	// Should track 3 followers
	if stats.FollowerCount != 3 {
		t.Errorf("expected 3 followers, got %d", stats.FollowerCount)
	}

	// Min should be 100
	if stats.MinIndex != 100 {
		t.Errorf("expected min index 100, got %d", stats.MinIndex)
	}

	// Max should be 200
	if stats.MaxIndex != 200 {
		t.Errorf("expected max index 200, got %d", stats.MaxIndex)
	}

	// Average lag should be (150+50+100)/3 = 100
	expectedAvgLag := 100.0
	if stats.AverageLag < expectedAvgLag-1 || stats.AverageLag > expectedAvgLag+1 {
		t.Errorf("expected average lag ~%f, got %f", expectedAvgLag, stats.AverageLag)
	}
}

func TestVersionTracker_ZeroIndex(t *testing.T) {
	lg := zaptest.NewLogger(t)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// Update follower to index 0 (should be valid)
	vt.UpdateFollower(types.ID(1), 0)

	// Should select for index 0
	id, ok := vt.SelectReplica(0)
	if !ok || id != types.ID(1) {
		t.Errorf("expected follower 1 to be selected for index 0, got ok=%v id=%d", ok, id)
	}
}

func BenchmarkVersionTracker_UpdateFollower(b *testing.B) {
	lg := zaptest.NewLogger(b)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vt.UpdateFollower(types.ID(i%10), uint64(i))
	}
}

func BenchmarkVersionTracker_SelectReplica(b *testing.B) {
	lg := zaptest.NewLogger(b)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// Setup: add 10 followers
	for i := 0; i < 10; i++ {
		vt.UpdateFollower(types.ID(i), 1000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vt.SelectReplica(1000)
	}
}

func BenchmarkVersionTracker_GetLag(b *testing.B) {
	lg := zaptest.NewLogger(b)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// Setup: add follower
	vt.UpdateFollower(types.ID(1), 500)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vt.GetLag(types.ID(1), 1000)
	}
}

func BenchmarkVersionTracker_ConcurrentOperations(b *testing.B) {
	lg := zaptest.NewLogger(b)
	vt := NewVersionTracker(lg, 1*time.Second, true)

	// Setup: add some followers
	for i := 0; i < 10; i++ {
		vt.UpdateFollower(types.ID(i), 1000)
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 3 {
			case 0:
				vt.UpdateFollower(types.ID(i%10), uint64(i))
			case 1:
				vt.SelectReplica(uint64(i))
			case 2:
				vt.GetLag(types.ID(i%10), uint64(i+100))
			}
			i++
		}
	})
}
