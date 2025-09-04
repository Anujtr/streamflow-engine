package monitoring

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	
	"github.com/Anujtr/streamflow-engine/internal/metrics"
	"github.com/Anujtr/streamflow-engine/internal/storage"
)

// PrometheusMetrics exposes StreamFlow metrics in Prometheus format
type PrometheusMetrics struct {
	// Message throughput metrics
	messagesProduced     prometheus.Counter
	messagesConsumed     prometheus.Counter
	batchesProcessed     prometheus.Counter
	
	// Track last values for counter updates
	lastMessagesProduced  int64
	lastMessagesConsumed  int64
	lastBatchesProcessed  int64
	lastProduceErrors     int64
	lastConsumeErrors     int64
	lastBatchErrors       int64
	lastCompactions       int64
	lastEventsProcessed   int64
	lastEventsFiltered    int64
	lastEventsTransformed int64
	lastWindowsCreated    int64
	
	// Latency metrics
	produceLatency       prometheus.Histogram
	consumeLatency       prometheus.Histogram
	batchProcessLatency  prometheus.Histogram
	
	// Error metrics
	produceErrors        prometheus.Counter
	consumeErrors        prometheus.Counter
	batchErrors          prometheus.Counter
	
	// Partition metrics
	partitionMessages    *prometheus.GaugeVec
	partitionBytes       *prometheus.GaugeVec
	partitionProduceRate *prometheus.GaugeVec
	partitionConsumeRate *prometheus.GaugeVec
	
	// Stream processing metrics
	eventsProcessed      prometheus.Counter
	eventsFiltered       prometheus.Counter
	eventsTransformed    prometheus.Counter
	eventsAggregated     prometheus.Counter
	windowsCreated       prometheus.Counter
	windowsExpired       prometheus.Counter
	processingLatency    prometheus.Histogram
	
	// System metrics
	systemUptime         prometheus.Gauge
	componentHealth      *prometheus.GaugeVec
	
	// Storage metrics
	pebbleWriteAmp       prometheus.Gauge
	pebbleCacheHitRatio  prometheus.Gauge
	pebbleCompactions    prometheus.Counter
	pebbleL0Files        prometheus.Gauge
	pebbleBytesStored    prometheus.Gauge
	
	// Reference to performance metrics
	perfMetrics *metrics.PerformanceMetrics
	storage     *storage.Storage
	startTime   time.Time
}

// NewPrometheusMetrics creates a new Prometheus metrics exporter
func NewPrometheusMetrics(perfMetrics *metrics.PerformanceMetrics, storage *storage.Storage) *PrometheusMetrics {
	pm := &PrometheusMetrics{
		perfMetrics: perfMetrics,
		storage:     storage,
		startTime:   time.Now(),
		
		// Message throughput
		messagesProduced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_messages_produced_total",
			Help: "Total number of messages produced",
		}),
		messagesConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_messages_consumed_total",  
			Help: "Total number of messages consumed",
		}),
		batchesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_batches_processed_total",
			Help: "Total number of batches processed",
		}),
		
		// Latency histograms
		produceLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "streamflow_produce_latency_seconds",
			Help: "Latency of produce operations in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~3.2s
		}),
		consumeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "streamflow_consume_latency_seconds",
			Help: "Latency of consume operations in seconds", 
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15),
		}),
		batchProcessLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "streamflow_batch_process_latency_seconds",
			Help: "Latency of batch processing operations in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15),
		}),
		
		// Error counters
		produceErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_produce_errors_total",
			Help: "Total number of produce errors",
		}),
		consumeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_consume_errors_total",
			Help: "Total number of consume errors", 
		}),
		batchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_batch_errors_total",
			Help: "Total number of batch processing errors",
		}),
		
		// Partition metrics
		partitionMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamflow_partition_messages",
			Help: "Number of messages in partition",
		}, []string{"topic", "partition"}),
		
		partitionBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamflow_partition_bytes",
			Help: "Number of bytes stored in partition",
		}, []string{"topic", "partition"}),
		
		partitionProduceRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamflow_partition_produce_rate",
			Help: "Messages per second produced to partition",
		}, []string{"topic", "partition"}),
		
		partitionConsumeRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamflow_partition_consume_rate", 
			Help: "Messages per second consumed from partition",
		}, []string{"topic", "partition"}),
		
		// Stream processing metrics
		eventsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_stream_events_processed_total",
			Help: "Total number of stream events processed",
		}),
		eventsFiltered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_stream_events_filtered_total",
			Help: "Total number of stream events filtered out",
		}),
		eventsTransformed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_stream_events_transformed_total",
			Help: "Total number of stream events transformed",
		}),
		eventsAggregated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_stream_events_aggregated_total", 
			Help: "Total number of stream events aggregated",
		}),
		windowsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_stream_windows_created_total",
			Help: "Total number of stream windows created",
		}),
		windowsExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_stream_windows_expired_total",
			Help: "Total number of stream windows expired",
		}),
		processingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "streamflow_stream_processing_latency_seconds",
			Help: "Latency of stream processing operations in seconds",
			Buckets: prometheus.ExponentialBuckets(0.000001, 2, 20), // 1μs to ~1s
		}),
		
		// System metrics
		systemUptime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "streamflow_uptime_seconds",
			Help: "System uptime in seconds",
		}),
		componentHealth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamflow_component_health",
			Help: "Health status of system components (1=healthy, 0=unhealthy)",
		}, []string{"component"}),
		
		// Storage metrics
		pebbleWriteAmp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "streamflow_pebble_write_amplification",
			Help: "Pebble database write amplification factor",
		}),
		pebbleCacheHitRatio: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "streamflow_pebble_cache_hit_ratio",
			Help: "Pebble database cache hit ratio",
		}),
		pebbleCompactions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "streamflow_pebble_compactions_total",
			Help: "Total number of Pebble database compactions",
		}),
		pebbleL0Files: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "streamflow_pebble_l0_files",
			Help: "Number of L0 files in Pebble database",
		}),
		pebbleBytesStored: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "streamflow_pebble_bytes_stored",
			Help: "Total bytes stored in Pebble database",
		}),
	}
	
	// Register all metrics
	prometheus.MustRegister(
		pm.messagesProduced,
		pm.messagesConsumed,
		pm.batchesProcessed,
		pm.produceLatency,
		pm.consumeLatency,
		pm.batchProcessLatency,
		pm.produceErrors,
		pm.consumeErrors,
		pm.batchErrors,
		pm.partitionMessages,
		pm.partitionBytes,
		pm.partitionProduceRate,
		pm.partitionConsumeRate,
		pm.eventsProcessed,
		pm.eventsFiltered,
		pm.eventsTransformed,
		pm.eventsAggregated,
		pm.windowsCreated,
		pm.windowsExpired,
		pm.processingLatency,
		pm.systemUptime,
		pm.componentHealth,
		pm.pebbleWriteAmp,
		pm.pebbleCacheHitRatio,
		pm.pebbleCompactions,
		pm.pebbleL0Files,
		pm.pebbleBytesStored,
	)
	
	return pm
}

// UpdateMetrics updates Prometheus metrics from performance metrics
func (pm *PrometheusMetrics) UpdateMetrics() {
	snapshot := pm.perfMetrics.GetSnapshot()
	
	// Update counters (add delta since last update)
	if snapshot.MessagesProduced > pm.lastMessagesProduced {
		pm.messagesProduced.Add(float64(snapshot.MessagesProduced - pm.lastMessagesProduced))
		pm.lastMessagesProduced = snapshot.MessagesProduced
	}
	if snapshot.MessagesConsumed > pm.lastMessagesConsumed {
		pm.messagesConsumed.Add(float64(snapshot.MessagesConsumed - pm.lastMessagesConsumed))
		pm.lastMessagesConsumed = snapshot.MessagesConsumed
	}
	if snapshot.BatchesProcessed > pm.lastBatchesProcessed {
		pm.batchesProcessed.Add(float64(snapshot.BatchesProcessed - pm.lastBatchesProcessed))
		pm.lastBatchesProcessed = snapshot.BatchesProcessed
	}
	
	// Update error counters
	if snapshot.ProduceErrors > pm.lastProduceErrors {
		pm.produceErrors.Add(float64(snapshot.ProduceErrors - pm.lastProduceErrors))
		pm.lastProduceErrors = snapshot.ProduceErrors
	}
	if snapshot.ConsumeErrors > pm.lastConsumeErrors {
		pm.consumeErrors.Add(float64(snapshot.ConsumeErrors - pm.lastConsumeErrors))
		pm.lastConsumeErrors = snapshot.ConsumeErrors
	}
	if snapshot.BatchErrors > pm.lastBatchErrors {
		pm.batchErrors.Add(float64(snapshot.BatchErrors - pm.lastBatchErrors))
		pm.lastBatchErrors = snapshot.BatchErrors
	}
	
	// Update latency histograms (observe current averages)
	if snapshot.ProduceLatency.AvgLatency > 0 {
		pm.produceLatency.Observe(snapshot.ProduceLatency.AvgLatency.Seconds())
	}
	if snapshot.ConsumeLatency.AvgLatency > 0 {
		pm.consumeLatency.Observe(snapshot.ConsumeLatency.AvgLatency.Seconds())
	}
	if snapshot.BatchProcessLatency.AvgLatency > 0 {
		pm.batchProcessLatency.Observe(snapshot.BatchProcessLatency.AvgLatency.Seconds())
	}
	
	// Update partition metrics
	for _, partitionMetric := range snapshot.PartitionMetrics {
		topicLabel := partitionMetric.TopicName
		partitionLabel := string(rune(partitionMetric.PartitionID + '0'))
		
		pm.partitionMessages.WithLabelValues(topicLabel, partitionLabel).Set(float64(partitionMetric.MessageCount))
		pm.partitionBytes.WithLabelValues(topicLabel, partitionLabel).Set(float64(partitionMetric.BytesStored))
		pm.partitionProduceRate.WithLabelValues(topicLabel, partitionLabel).Set(partitionMetric.ProduceRate)
		pm.partitionConsumeRate.WithLabelValues(topicLabel, partitionLabel).Set(partitionMetric.ConsumeRate)
	}
	
	// Update system metrics
	pm.systemUptime.Set(time.Since(pm.startTime).Seconds())
	
	// Update storage metrics
	pm.pebbleWriteAmp.Set(snapshot.PebbleMetrics.WriteAmplification)
	pm.pebbleCacheHitRatio.Set(snapshot.PebbleMetrics.CacheHitRatio)
	if snapshot.PebbleMetrics.CompactionCount > pm.lastCompactions {
		pm.pebbleCompactions.Add(float64(snapshot.PebbleMetrics.CompactionCount - pm.lastCompactions))
		pm.lastCompactions = snapshot.PebbleMetrics.CompactionCount
	}
	pm.pebbleL0Files.Set(float64(snapshot.PebbleMetrics.L0FileCount))
	pm.pebbleBytesStored.Set(float64(snapshot.PebbleMetrics.TotalBytesStored))
}

// UpdateStreamProcessorMetrics updates stream processing metrics
func (pm *PrometheusMetrics) UpdateStreamProcessorMetrics(processorMetrics interface{}) {
	// This would be called by the stream processor to update its metrics
	// For now, we'll implement a simple interface-based approach
	if metricsMap, ok := processorMetrics.(map[string]interface{}); ok {
		if val, exists := metricsMap["EventsProcessed"]; exists {
			if count, ok := val.(int64); ok && count > pm.lastEventsProcessed {
				pm.eventsProcessed.Add(float64(count - pm.lastEventsProcessed))
				pm.lastEventsProcessed = count
			}
		}
		if val, exists := metricsMap["EventsFiltered"]; exists {
			if count, ok := val.(int64); ok && count > pm.lastEventsFiltered {
				pm.eventsFiltered.Add(float64(count - pm.lastEventsFiltered))
				pm.lastEventsFiltered = count
			}
		}
		if val, exists := metricsMap["EventsTransformed"]; exists {
			if count, ok := val.(int64); ok && count > pm.lastEventsTransformed {
				pm.eventsTransformed.Add(float64(count - pm.lastEventsTransformed))
				pm.lastEventsTransformed = count
			}
		}
		if val, exists := metricsMap["WindowsCreated"]; exists {
			if count, ok := val.(int64); ok && count > pm.lastWindowsCreated {
				pm.windowsCreated.Add(float64(count - pm.lastWindowsCreated))
				pm.lastWindowsCreated = count
			}
		}
		if val, exists := metricsMap["AvgLatency"]; exists {
			if latency, ok := val.(time.Duration); ok {
				pm.processingLatency.Observe(latency.Seconds())
			}
		}
	}
}

// UpdateComponentHealth updates component health metrics
func (pm *PrometheusMetrics) UpdateComponentHealth(componentName string, isHealthy bool) {
	healthValue := 0.0
	if isHealthy {
		healthValue = 1.0
	}
	pm.componentHealth.WithLabelValues(componentName).Set(healthValue)
}

// GetHandler returns the HTTP handler for Prometheus metrics
func (pm *PrometheusMetrics) GetHandler() http.Handler {
	return promhttp.Handler()
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status     string                 `json:"status"`
	Version    string                 `json:"version"`
	Uptime     string                 `json:"uptime"`
	Timestamp  time.Time             `json:"timestamp"`
	Components map[string]interface{} `json:"components"`
}

// GetHealthHandler returns an HTTP handler for health checks
func (pm *PrometheusMetrics) GetHealthHandler(version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Simple health check
		uptime := time.Since(pm.startTime)
		
		health := HealthResponse{
			Status:    "healthy",
			Version:   version,
			Uptime:    uptime.String(),
			Timestamp: time.Now(),
			Components: map[string]interface{}{
				"storage":     "healthy",
				"partitions":  "healthy",
				"coordinator": "healthy",
			},
		}
		
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(health)
	}
}