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
	"github.com/prometheus/client_golang/prometheus"
)

// IONIA-inspired optimization metrics
var (
	// ReadIndex Cache metrics
	readIndexCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "readindex_cache_hits_total",
		Help:      "Total number of ReadIndex cache hits (0-RTT reads).",
	})
	readIndexCacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "readindex_cache_misses_total",
		Help:      "Total number of ReadIndex cache misses.",
	})
	readIndexCacheInvalidations = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "readindex_cache_invalidations_total",
		Help:      "Total number of ReadIndex cache invalidations (leadership changes).",
	})
	readIndexCacheHitRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "readindex_cache_hit_rate",
		Help:      "Current ReadIndex cache hit rate (0.0 to 1.0).",
	})
	readIndexCacheAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "readindex_cache_age_seconds",
		Help:      "Age of the current ReadIndex cache entry in seconds.",
	})

	// Version Tracker metrics
	versionTrackerFollowers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "version_tracker_followers_total",
		Help:      "Number of followers currently tracked for version-aware reads.",
	})
	versionTrackerStaleFollowers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "version_tracker_stale_followers_total",
		Help:      "Number of stale followers (haven't sent heartbeat recently).",
	})
	versionTrackerReplicaSelections = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "version_tracker_replica_selections_total",
		Help:      "Total number of successful replica selections for reads.",
	})
	versionTrackerSelectionFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "version_tracker_selection_failures_total",
		Help:      "Total number of failed replica selections (no replicas caught up).",
	})
	versionTrackerAverageLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "version_tracker_average_lag_entries",
		Help:      "Average replication lag across all followers in entries.",
	})
	versionTrackerMaxLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "version_tracker_max_lag_entries",
		Help:      "Maximum replication lag among all followers in entries.",
	})

	// Parallel Replication metrics
	parallelSendBatches = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "parallel_send_batches_total",
		Help:      "Total number of message batches sent in parallel.",
	})
	parallelSendMessages = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "parallel_send_messages_total",
		Help:      "Total number of messages sent using parallel send.",
	})
	parallelSendDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "parallel_send_duration_seconds",
		Help:      "Histogram of parallel send operation durations in seconds.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14), // 0.1ms to ~1.6s
	})

	// Smart Follower Read metrics
	smartFollowerReadsFastPath = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "smart_follower_reads_fast_path_total",
		Help:      "Total number of smart follower reads using fast path (0-RTT cache hit).",
	})
	smartFollowerReadsSlowPath = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "smart_follower_reads_slow_path_total",
		Help:      "Total number of smart follower reads using slow path (cache miss).",
	})

	// IONIA feature enablement metrics
	ioniaFeatureEnabled = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "etcd",
			Subsystem: "server",
			Name:      "ionia_feature_enabled",
			Help:      "Whether an IONIA feature is enabled. 1 if enabled, 0 otherwise.",
		},
		[]string{"feature"},
	)
)

func init() {
	// Register ReadIndex Cache metrics
	prometheus.MustRegister(readIndexCacheHits)
	prometheus.MustRegister(readIndexCacheMisses)
	prometheus.MustRegister(readIndexCacheInvalidations)
	prometheus.MustRegister(readIndexCacheHitRate)
	prometheus.MustRegister(readIndexCacheAge)

	// Register Version Tracker metrics
	prometheus.MustRegister(versionTrackerFollowers)
	prometheus.MustRegister(versionTrackerStaleFollowers)
	prometheus.MustRegister(versionTrackerReplicaSelections)
	prometheus.MustRegister(versionTrackerSelectionFailures)
	prometheus.MustRegister(versionTrackerAverageLag)
	prometheus.MustRegister(versionTrackerMaxLag)

	// Register Parallel Replication metrics
	prometheus.MustRegister(parallelSendBatches)
	prometheus.MustRegister(parallelSendMessages)
	prometheus.MustRegister(parallelSendDuration)

	// Register Smart Follower Read metrics
	prometheus.MustRegister(smartFollowerReadsFastPath)
	prometheus.MustRegister(smartFollowerReadsSlowPath)

	// Register IONIA feature metrics
	prometheus.MustRegister(ioniaFeatureEnabled)
}
