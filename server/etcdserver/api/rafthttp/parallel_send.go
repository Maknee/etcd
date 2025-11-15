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

package rafthttp

import (
	"sync"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3/raftpb"
)

// SendParallel sends messages to peers in parallel for reduced latency
// and better SSD I/O utilization. This is inspired by IONIA's parallel
// replication approach.
//
// Unlike the sequential Send method, this sends all messages concurrently,
// which is particularly beneficial for:
// - Reducing critical path latency in Raft replication
// - Better utilizing modern SSD parallel I/O capabilities
// - Improving write throughput in multi-follower scenarios
func (t *Transport) SendParallel(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}

	// For single message, no parallelism needed
	if len(msgs) == 1 {
		t.Send(msgs)
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(msgs))

	for _, m := range msgs {
		go func(msg raftpb.Message) {
			defer wg.Done()

			if msg.To == 0 {
				// ignore intentionally dropped message
				return
			}

			to := types.ID(msg.To)

			t.mu.RLock()
			p, pok := t.peers[to]
			g, rok := t.remotes[to]
			t.mu.RUnlock()

			if pok {
				if isMsgApp(msg) {
					t.ServerStats.SendAppendReq(msg.Size())
				}
				p.send(msg)
				return
			}

			if rok {
				g.send(msg)
				return
			}

			if t.Logger != nil && t.Logger.Core().Enabled(zap.DebugLevel) {
				t.Logger.Debug("parallel send: peer not found",
					zap.String("peer-id", to.String()),
					zap.String("msg-type", msg.Type.String()),
				)
			}
		}(m)
	}

	// Wait for all sends to complete
	wg.Wait()
}

// SendParallelWithStats is like SendParallel but also returns timing statistics
// This is useful for monitoring and tuning parallel send performance
type ParallelSendStats struct {
	MessageCount int
	SuccessCount int
	FailureCount int
}

func (t *Transport) SendParallelWithStats(msgs []raftpb.Message) ParallelSendStats {
	stats := ParallelSendStats{
		MessageCount: len(msgs),
	}

	if len(msgs) == 0 {
		return stats
	}

	if len(msgs) == 1 {
		t.Send(msgs)
		stats.SuccessCount = 1
		return stats
	}

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)

	wg.Add(len(msgs))

	for _, m := range msgs {
		go func(msg raftpb.Message) {
			defer wg.Done()

			if msg.To == 0 {
				mu.Lock()
				stats.FailureCount++
				mu.Unlock()
				return
			}

			to := types.ID(msg.To)

			t.mu.RLock()
			p, pok := t.peers[to]
			g, rok := t.remotes[to]
			t.mu.RUnlock()

			sent := false

			if pok {
				if isMsgApp(msg) {
					t.ServerStats.SendAppendReq(msg.Size())
				}
				p.send(msg)
				sent = true
			} else if rok {
				g.send(msg)
				sent = true
			}

			mu.Lock()
			if sent {
				stats.SuccessCount++
			} else {
				stats.FailureCount++
			}
			mu.Unlock()
		}(m)
	}

	wg.Wait()
	return stats
}

// ShouldUseParallelSend determines whether parallel sending should be used
// based on the message batch characteristics
func ShouldUseParallelSend(msgs []raftpb.Message) bool {
	// Only use parallel send for multiple messages
	if len(msgs) <= 1 {
		return false
	}

	// Check if messages are going to different peers
	// If all messages go to the same peer, parallel send doesn't help
	firstTo := msgs[0].To
	for i := 1; i < len(msgs); i++ {
		if msgs[i].To != firstTo {
			// Multiple different destinations - parallel send beneficial
			return true
		}
	}

	// All messages to same peer - sequential is fine
	return false
}
