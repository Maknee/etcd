# IONIA Prometheus Metrics

**Status:** Complete
**Date:** 2025-11-16
**Purpose:** Monitor and observe IONIA-inspired optimizations in etcd

---

## Overview

This document describes all Prometheus metrics exposed by the IONIA optimizations in etcd. These metrics enable monitoring, alerting, and performance tuning of the IONIA features.

All IONIA metrics use the `etcd_server_*` namespace to maintain consistency with existing etcd metrics.

---

## ReadIndex Cache Metrics

These metrics track the performance and behavior of the ReadIndex cache, which enables 0-RTT follower reads.

### Counters

| Metric | Type | Description |
|--------|------|-------------|
| `etcd_server_readindex_cache_hits_total` | Counter | Total number of ReadIndex cache hits (0-RTT reads) |
| `etcd_server_readindex_cache_misses_total` | Counter | Total number of ReadIndex cache misses |
| `etcd_server_readindex_cache_invalidations_total` | Counter | Total number of cache invalidations (leadership changes) |

### Gauges

| Metric | Type | Description |
|--------|------|-------------|
| `etcd_server_readindex_cache_hit_rate` | Gauge | Current cache hit rate (0.0 to 1.0) |
| `etcd_server_readindex_cache_age_seconds` | Gauge | Age of current cache entry in seconds |

### Usage Example

```promql
# Cache hit rate over 5 minutes
rate(etcd_server_readindex_cache_hits_total[5m]) /
(rate(etcd_server_readindex_cache_hits_total[5m]) +
 rate(etcd_server_readindex_cache_misses_total[5m]))

# Cache invalidations per hour
increase(etcd_server_readindex_cache_invalidations_total[1h])
```

### Expected Values

- **Hit rate**: >80% under normal load indicates good performance
- **Invalidations**: Should be low and correlate with leader elections
- **Cache age**: Should stay within configured lease duration (default: 50ms)

---

## Version Tracker Metrics

These metrics monitor follower replication status and replica selection for version-aware reads.

### Counters

| Metric | Type | Description |
|--------|------|-------------|
| `etcd_server_version_tracker_replica_selections_total` | Counter | Total successful replica selections for reads |
| `etcd_server_version_tracker_selection_failures_total` | Counter | Total failed selections (no replicas caught up) |

### Gauges

| Metric | Type | Description |
|--------|------|-------------|
| `etcd_server_version_tracker_followers_total` | Gauge | Number of followers currently tracked |
| `etcd_server_version_tracker_stale_followers_total` | Gauge | Number of stale followers (no recent heartbeat) |
| `etcd_server_version_tracker_average_lag_entries` | Gauge | Average replication lag across all followers in entries |
| `etcd_server_version_tracker_max_lag_entries` | Gauge | Maximum lag among all followers in entries |

### Usage Example

```promql
# Selection success rate
rate(etcd_server_version_tracker_replica_selections_total[5m]) /
(rate(etcd_server_version_tracker_replica_selections_total[5m]) +
 rate(etcd_server_version_tracker_selection_failures_total[5m]))

# Alert on high replication lag
etcd_server_version_tracker_max_lag_entries > 1000

# Alert on stale followers
etcd_server_version_tracker_stale_followers_total > 0
```

### Expected Values

- **Followers tracked**: Should equal cluster size minus 1 (leader not tracked)
- **Stale followers**: Should be 0 under normal conditions
- **Average lag**: <100 entries is healthy, <10 is excellent
- **Max lag**: <500 entries indicates good cluster health
- **Selection failures**: Should be rare (<1% of total selections)

---

## Parallel Replication Metrics

These metrics track the use and performance of parallel message sending in Raft.

### Counters

| Metric | Type | Description |
|--------|------|-------------|
| `etcd_server_parallel_send_batches_total` | Counter | Total message batches sent in parallel |
| `etcd_server_parallel_send_messages_total` | Counter | Total individual messages sent using parallel send |

### Histograms

| Metric | Type | Description |
|--------|------|-------------|
| `etcd_server_parallel_send_duration_seconds` | Histogram | Duration of parallel send operations |

**Histogram buckets:** 0.1ms to ~1.6s (exponential)

### Usage Example

```promql
# Average messages per batch
rate(etcd_server_parallel_send_messages_total[5m]) /
rate(etcd_server_parallel_send_batches_total[5m])

# 99th percentile parallel send duration
histogram_quantile(0.99,
  rate(etcd_server_parallel_send_duration_seconds_bucket[5m]))

# Parallel send usage rate (as % of all writes)
rate(etcd_server_parallel_send_batches_total[5m]) /
rate(etcd_server_proposals_committed_total[5m])
```

### Expected Values

- **Messages per batch**: ~2-5 in typical 3-5 node clusters
- **p99 duration**: <10ms for healthy clusters
- **Usage rate**: Depends on write rate and cluster size

---

## Smart Follower Read Metrics

These metrics track the optimization of follower reads using ReadIndex caching.

### Counters

| Metric | Type | Description |
|--------|------|-------------|
| `etcd_server_smart_follower_reads_fast_path_total` | Counter | Reads using fast path (0-RTT cache hit) |
| `etcd_server_smart_follower_reads_slow_path_total` | Counter | Reads using slow path (cache miss) |

### Usage Example

```promql
# Fast path usage rate
rate(etcd_server_smart_follower_reads_fast_path_total[5m]) /
(rate(etcd_server_smart_follower_reads_fast_path_total[5m]) +
 rate(etcd_server_smart_follower_reads_slow_path_total[5m]))

# Reads benefiting from IONIA optimizations
rate(etcd_server_smart_follower_reads_fast_path_total[5m])
```

### Expected Values

- **Fast path rate**: Should match ReadIndex cache hit rate (>80%)
- **Total smart reads**: All linearizable reads on followers (when enabled)

---

## Feature Enablement Metrics

Track which IONIA features are currently enabled.

### Gauges

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `etcd_server_ionia_feature_enabled` | Gauge | `feature` | Whether a feature is enabled (1) or not (0) |

**Feature labels:**
- `version_tracking` - Version tracker enabled
- `smart_follower_reads` - Smart follower reads enabled
- `parallel_replication` - Parallel replication enabled

### Usage Example

```promql
# Check if smart follower reads are enabled
etcd_server_ionia_feature_enabled{feature="smart_follower_reads"}

# Count clusters with parallel replication enabled
count(etcd_server_ionia_feature_enabled{feature="parallel_replication"} == 1)
```

---

## Alerting Examples

### Critical Alerts

```yaml
# High cache miss rate
- alert: IONIALowCacheHitRate
  expr: |
    rate(etcd_server_readindex_cache_hits_total[5m]) /
    (rate(etcd_server_readindex_cache_hits_total[5m]) +
     rate(etcd_server_readindex_cache_misses_total[5m])) < 0.5
  for: 10m
  annotations:
    summary: "IONIA cache hit rate below 50%"
    description: "ReadIndex cache hit rate is {{ $value }}, indicating potential issues"

# High replication lag
- alert: IONIAHighReplicationLag
  expr: etcd_server_version_tracker_max_lag_entries > 1000
  for: 5m
  annotations:
    summary: "High replication lag detected"
    description: "Maximum follower lag is {{ $value }} entries"

# Stale followers
- alert: IONIAStaleFollowers
  expr: etcd_server_version_tracker_stale_followers_total > 0
  for: 2m
  annotations:
    summary: "Stale followers detected"
    description: "{{ $value }} followers haven't sent heartbeat recently"
```

### Warning Alerts

```yaml
# Increasing selection failures
- alert: IONIAReplicaSelectionFailures
  expr: |
    rate(etcd_server_version_tracker_selection_failures_total[5m]) /
    (rate(etcd_server_version_tracker_replica_selections_total[5m]) +
     rate(etcd_server_version_tracker_selection_failures_total[5m])) > 0.1
  for: 5m
  annotations:
    summary: "High replica selection failure rate"
    description: "{{ $value | humanizePercentage }} of selections are failing"

# Frequent cache invalidations
- alert: IONIAFrequentCacheInvalidations
  expr: rate(etcd_server_readindex_cache_invalidations_total[1h]) > 10
  for: 10m
  annotations:
    summary: "Frequent ReadIndex cache invalidations"
    description: "Cache invalidated {{ $value }} times/sec (frequent leader changes?)"
```

---

## Dashboarding

### Recommended Panels

**1. ReadIndex Cache Performance**
- Cache hit rate (gauge)
- Hits vs misses (stacked area chart)
- Invalidations (counter)
- Cache age (time series)

**2. Replication Status**
- Follower count (gauge)
- Average & max lag (time series)
- Stale followers (gauge)
- Replica selections (counter)

**3. Parallel Replication**
- Batches sent (counter)
- Messages per batch (calculated)
- Send duration histogram

**4. Smart Follower Reads**
- Fast vs slow path (stacked area)
- Fast path percentage (gauge)

**5. Feature Status**
- Feature enablement matrix (table)

### Grafana Dashboard Example

```json
{
  "title": "IONIA Performance",
  "panels": [
    {
      "title": "ReadIndex Cache Hit Rate",
      "targets": [{
        "expr": "rate(etcd_server_readindex_cache_hits_total[5m]) / (rate(etcd_server_readindex_cache_hits_total[5m]) + rate(etcd_server_readindex_cache_misses_total[5m]))"
      }],
      "type": "gauge",
      "thresholds": {
        "mode": "absolute",
        "steps": [
          {"value": 0, "color": "red"},
          {"value": 0.7, "color": "yellow"},
          {"value": 0.8, "color": "green"}
        ]
      }
    },
    {
      "title": "Replication Lag",
      "targets": [
        {"expr": "etcd_server_version_tracker_average_lag_entries", "legendFormat": "Average"},
        {"expr": "etcd_server_version_tracker_max_lag_entries", "legendFormat": "Maximum"}
      ],
      "type": "graph"
    }
  ]
}
```

---

## Performance Tuning

### Using Metrics for Tuning

**1. Cache Duration Optimization**

If cache hit rate is low but invalidations are rare:
```promql
# Check average time between invalidations
1 / rate(etcd_server_readindex_cache_invalidations_total[1h])
```

If this is >5x your current cache duration, consider increasing `read-index-cache-duration`.

**2. Stale Threshold Tuning**

If you see stale followers but they're actually healthy:
```promql
# Check actual heartbeat intervals
changes(etcd_server_version_tracker_followers_total[5m])
```

Increase `version-tracker-stale-threshold` if needed.

**3. Parallel Send Effectiveness**

Check if parallel send is improving latency:
```promql
# Compare to baseline write latency
histogram_quantile(0.99, rate(etcd_server_parallel_send_duration_seconds_bucket[5m]))
vs
histogram_quantile(0.99, rate(etcd_disk_backend_commit_duration_seconds_bucket[5m]))
```

---

## Troubleshooting

### Low Cache Hit Rate

**Symptoms:** `etcd_server_readindex_cache_hit_rate < 0.7`

**Possible causes:**
1. Frequent leader changes → Check `etcd_server_readindex_cache_invalidations_total`
2. Cache duration too short → Increase `read-index-cache-duration`
3. Mixed read patterns (leader + follower reads)

### High Replication Lag

**Symptoms:** `etcd_server_version_tracker_max_lag_entries > 500`

**Possible causes:**
1. Slow followers → Check disk I/O metrics
2. Network issues → Check network latency
3. High write load → Check `etcd_server_proposals_committed_total`

### Selection Failures

**Symptoms:** `etcd_server_version_tracker_selection_failures_total` increasing

**Possible causes:**
1. All followers lagging → Check replication lag
2. Stale followers → Check `etcd_server_version_tracker_stale_followers_total`
3. Single follower cluster → Expected (need 2+ followers)

---

## Integration with Existing Metrics

IONIA metrics complement existing etcd metrics:

| IONIA Metric | Related etcd Metric | Use Case |
|--------------|---------------------|----------|
| `readindex_cache_*` | `read_indexes_failed_total` | ReadIndex performance |
| `version_tracker_*_lag` | `proposals_committed_total` | Replication health |
| `parallel_send_*` | `proposals_pending` | Write performance |
| `smart_follower_reads_*` | `slow_read_indexes_total` | Read optimization |

---

## Metric Collection Frequency

**Recommended scrape interval:** 15-30 seconds

**Retention recommendations:**
- **Short-term (high resolution):** 7 days at 15s intervals
- **Long-term (rollup):** 90 days at 5m intervals

---

## Summary

### Key Metrics to Monitor

**For Production:**
1. `etcd_server_readindex_cache_hit_rate` - Should be >80%
2. `etcd_server_version_tracker_max_lag_entries` - Should be <500
3. `etcd_server_version_tracker_stale_followers_total` - Should be 0
4. `etcd_server_ionia_feature_enabled` - Track feature status

**For Performance Analysis:**
1. `etcd_server_parallel_send_duration_seconds` - p99 latency
2. `etcd_server_smart_follower_reads_fast_path_total` - Optimization impact
3. `etcd_server_version_tracker_replica_selections_total` - Load distribution

**For Debugging:**
1. `etcd_server_readindex_cache_invalidations_total` - Leader stability
2. `etcd_server_version_tracker_selection_failures_total` - Availability issues
3. `etcd_server_readindex_cache_age_seconds` - Cache freshness

---

**Last Updated:** 2025-11-16
**Implementation:** IONIA Phase 1
**Metrics File:** `server/etcdserver/ionia_metrics.go`
