package stream

import (
	"context"
	"time"
)

// Stream represents a stream of events with fluent API operations
type Stream interface {
	// Transformation operations
	Filter(FilterFunc) Stream
	Map(MapFunc) Stream
	
	// Windowing operations
	Window(duration time.Duration) WindowedStream
	WindowConfig(config *WindowConfig) WindowedStream
	SessionWindow(timeout time.Duration) WindowedStream
	SlidingWindow(size, slide time.Duration) WindowedStream
	
	// Grouping operations
	GroupBy(KeyExtractorFunc) GroupedStream
	
	// Join operations
	Join(otherStream Stream, joinFunc JoinFunc, windowSize time.Duration) JoinedStream
	LeftJoin(otherStream Stream, joinFunc JoinFunc, windowSize time.Duration) JoinedStream
	Enrich(enrichmentSource EnrichmentSource, keyExtractor KeyExtractorFunc) Stream
	
	// Event time and watermarks
	WithEventTime(timeExtractor TimeExtractorFunc) Stream
	WithWatermark(watermarkGenerator func(*Event) *Watermark) Stream
	
	// Pattern detection
	Detect(patternMatcher PatternMatcherFunc, withinTime time.Duration) PatternStream
	
	// Flow control
	WithBackpressure(config *FlowControlConfig) Stream
	
	// Deduplication
	Deduplicate(keyExtractor KeyExtractorFunc, withinTime time.Duration) Stream
	
	// Terminal operations
	ForEach(func(*Event)) error
	Output(topic string) error
	
	// Control operations
	Start(ctx context.Context) error
	Stop() error
	
	// Metrics and monitoring
	GetMetrics() *ProcessorMetrics
}

// WindowedStream represents a stream with windowing applied
type WindowedStream interface {
	// Grouping operations on windowed stream
	GroupBy(KeyExtractorFunc) WindowedGroupedStream
	
	// Aggregation operations
	Count() AggregatedStream
	Sum(ValueExtractorFunc) AggregatedStream
	Average(ValueExtractorFunc) AggregatedStream
	Min(ValueExtractorFunc) AggregatedStream
	Max(ValueExtractorFunc) AggregatedStream
	Reduce(AggregatorFunc) AggregatedStream
	
	// Terminal operations
	ForEach(func(*WindowedEvent)) error
	Output(topic string) error
	
	// Control operations
	Start(ctx context.Context) error
	Stop() error
	
	// Metrics and monitoring
	GetMetrics() *ProcessorMetrics
}

// GroupedStream represents a stream with grouping applied
type GroupedStream interface {
	// Windowing operations on grouped stream
	Window(duration time.Duration) WindowedGroupedStream
	SessionWindow(timeout time.Duration) WindowedGroupedStream
	
	// Aggregation operations
	Count() AggregatedStream
	Sum(ValueExtractorFunc) AggregatedStream
	Average(ValueExtractorFunc) AggregatedStream
	Min(ValueExtractorFunc) AggregatedStream
	Max(ValueExtractorFunc) AggregatedStream
	Reduce(AggregatorFunc) AggregatedStream
	
	// Terminal operations
	ForEach(func(string, []*Event)) error
	Output(topic string) error
	
	// Control operations
	Start(ctx context.Context) error
	Stop() error
	
	// Metrics and monitoring
	GetMetrics() *ProcessorMetrics
}

// WindowedGroupedStream represents a stream with both windowing and grouping applied
type WindowedGroupedStream interface {
	// Aggregation operations
	Count() AggregatedStream
	Sum(ValueExtractorFunc) AggregatedStream
	Average(ValueExtractorFunc) AggregatedStream
	Min(ValueExtractorFunc) AggregatedStream
	Max(ValueExtractorFunc) AggregatedStream
	Reduce(AggregatorFunc) AggregatedStream
	
	// Terminal operations
	ForEach(func(string, *Window, []*Event)) error
	Output(topic string) error
	
	// Control operations
	Start(ctx context.Context) error
	Stop() error
	
	// Metrics and monitoring
	GetMetrics() *ProcessorMetrics
}

// AggregatedStream represents a stream with aggregation applied
type AggregatedStream interface {
	// Transformation operations on aggregated results
	FilterResults(func(*AggregateResult) bool) AggregatedStream
	MapResults(func(*AggregateResult) *AggregateResult) AggregatedStream
	
	// Terminal operations
	ForEach(func(*AggregateResult)) error
	Output(topic string) error
	
	// Control operations
	Start(ctx context.Context) error
	Stop() error
	
	// Metrics and monitoring
	GetMetrics() *ProcessorMetrics
}

// StreamProcessor manages the lifecycle and execution of stream processing pipelines
type StreamProcessor interface {
	// Stream creation
	NewStream(inputTopic string) Stream
	
	// Processor management
	Start(ctx context.Context) error
	Stop() error
	IsRunning() bool
	
	// Configuration and state
	GetConfig() *ProcessorConfig
	GetMetrics() *ProcessorMetrics
	GetHealthStatus() string
}

// StateStore interface for managing stateful processing state
type StateStore interface {
	// State management
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	
	// Range operations
	GetRange(ctx context.Context, startKey, endKey string) (map[string][]byte, error)
	
	// Window state management
	GetWindowState(ctx context.Context, window *Window, key string) ([]byte, error)
	PutWindowState(ctx context.Context, window *Window, key string, value []byte) error
	DeleteWindowState(ctx context.Context, window *Window, key string) error
	
	// Window lifecycle
	ExpireWindow(ctx context.Context, window *Window) error
	ListActiveWindows(ctx context.Context) ([]*Window, error)
	
	// Maintenance
	Close() error
	Flush() error
}

// WindowManager manages the lifecycle of time-based windows
type WindowManager interface {
	// Window creation and management
	GetWindow(timestamp time.Time) *Window
	GetActiveWindows() []*Window
	ExpireWindows(cutoffTime time.Time) ([]*Window, error)
	
	// Window configuration
	GetWindowSize() time.Duration
	GetWindowSlide() time.Duration
	
	// Lifecycle
	Start(ctx context.Context) error
	Stop() error
}

// JoinedStream represents a stream resulting from joining two streams
type JoinedStream interface {
	// Transformation operations on joined results
	Filter(func(*JoinResult) bool) JoinedStream
	Map(func(*JoinResult) *Event) Stream
	
	// Windowing operations
	Window(duration time.Duration) WindowedStream
	
	// Terminal operations
	ForEach(func(*JoinResult)) error
	Output(topic string) error
	
	// Control operations
	Start(ctx context.Context) error
	Stop() error
	
	// Metrics and monitoring
	GetMetrics() *ProcessorMetrics
}

// PatternStream represents a stream for pattern detection results
type PatternStream interface {
	// Transformation operations on pattern results
	Filter(func(*PatternResult) bool) PatternStream
	Map(func(*PatternResult) *Event) Stream
	
	// Terminal operations
	ForEach(func(*PatternResult)) error
	Output(topic string) error
	Alert(alertTopic string) error
	
	// Control operations
	Start(ctx context.Context) error
	Stop() error
	
	// Metrics and monitoring
	GetMetrics() *ProcessorMetrics
}

// WatermarkManager manages watermarks for event-time processing
type WatermarkManager interface {
	// Watermark operations
	UpdateWatermark(source string, timestamp time.Time)
	GetWatermark(source string) *Watermark
	GetGlobalWatermark() *Watermark
	
	// Late event handling
	IsLateEvent(event *Event, allowedLateness time.Duration) bool
	HandleLateEvent(event *Event) error
	
	// Lifecycle
	Start(ctx context.Context) error
	Stop() error
}

// PatternDetector handles complex event pattern detection
type PatternDetector interface {
	// Pattern operations
	AddPattern(name string, matcher PatternMatcherFunc, withinTime time.Duration) error
	RemovePattern(name string) error
	
	// Event processing
	ProcessEvent(event *Event) ([]*PatternResult, error)
	
	// State management
	GetPatternState(patternName string) (map[string]interface{}, error)
	ClearPatternState(patternName string) error
	
	// Lifecycle
	Start(ctx context.Context) error
	Stop() error
}

// FlowController manages backpressure and flow control
type FlowController interface {
	// Flow control operations
	CanProcess() bool
	OnEventProcessed()
	OnEventDropped()
	OnEventBuffered()
	
	// Backpressure management
	ApplyBackpressure(event *Event) error
	GetCurrentThroughput() float64
	GetBufferUtilization() float64
	
	// Circuit breaker
	IsCircuitOpen() bool
	RecordSuccess()
	RecordFailure()
	
	// Configuration
	UpdateConfig(config *FlowControlConfig) error
	GetConfig() *FlowControlConfig
	
	// Lifecycle
	Start(ctx context.Context) error
	Stop() error
}

// ProcessorConfig holds configuration for stream processing
type ProcessorConfig struct {
	ProcessorName    string        `json:"processor_name"`
	InputTopic       string        `json:"input_topic"`
	OutputTopic      string        `json:"output_topic,omitempty"`
	ConsumerGroup    string        `json:"consumer_group"`
	
	// Performance settings
	MaxConcurrency   int           `json:"max_concurrency"`
	BatchSize        int           `json:"batch_size"`
	FlushInterval    time.Duration `json:"flush_interval"`
	
	// Windowing settings
	WindowSize       time.Duration `json:"window_size,omitempty"`
	WindowSlide      time.Duration `json:"window_slide,omitempty"`
	WindowRetention  time.Duration `json:"window_retention,omitempty"`
	
	// State settings
	StateStorePath   string        `json:"state_store_path,omitempty"`
	StateStoreType   string        `json:"state_store_type"` // "pebble", "memory"
	
	// Event time settings
	EventTimeExtractor TimeExtractorFunc `json:"-"`
	WatermarkConfig    *WatermarkConfig  `json:"watermark_config,omitempty"`
	
	// Flow control settings
	FlowControlConfig *FlowControlConfig `json:"flow_control_config,omitempty"`
	
	// Error handling
	ErrorTopic       string        `json:"error_topic,omitempty"`
	RetryAttempts    int           `json:"retry_attempts"`
	RetryBackoff     time.Duration `json:"retry_backoff"`
}

// WatermarkConfig holds configuration for watermark generation
type WatermarkConfig struct {
	MaxOutOfOrderness time.Duration `json:"max_out_of_orderness"`
	IdleSourceTimeout time.Duration `json:"idle_source_timeout"`
	WatermarkInterval time.Duration `json:"watermark_interval"`
}