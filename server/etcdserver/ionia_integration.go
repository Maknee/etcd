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
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3"
)

// updateVersionTrackerFromRaftStatus updates the version tracker based on
// the current raft status. This should be called periodically by the leader
// to track follower progress.
//
// This is part of the IONIA-inspired optimizations for smart read routing.
func (s *EtcdServer) updateVersionTrackerFromRaftStatus(status raft.Status) {
	if s.versionTracker == nil || !s.versionTracker.IsEnabled() {
		return
	}

	// Only update when we're the leader
	if status.Lead != uint64(s.MemberID()) {
		return
	}

	// Update each follower's match index
	for id, progress := range status.Progress {
		if id == status.ID {
			// Skip ourselves
			continue
		}

		followerID := types.ID(id)
		matchIndex := progress.Match

		s.versionTracker.UpdateFollower(followerID, matchIndex)
	}
}

// invalidateReadIndexCacheOnLeaderChange invalidates the ReadIndex cache
// when leadership changes to ensure linearizability.
//
// This is part of the IONIA-inspired optimizations for smart follower reads.
func (s *EtcdServer) invalidateReadIndexCacheOnLeaderChange() {
	if s.readIndexCache == nil || !s.readIndexCache.IsEnabled() {
		return
	}

	s.readIndexCache.Invalidate()
	s.Logger().Info("invalidated readindex cache due to leader change")
}
