package stream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	CircuitBreakerClosed CircuitBreakerState = iota
	CircuitBreakerOpen
	CircuitBreakerHalfOpen
)

// CircuitBreaker implements circuit breaker pattern for flow control
type CircuitBreaker struct {
	maxFailures     int32
	resetTimeout    time.Duration
	currentFailures int32
	lastFailureTime int64
	state           CircuitBreakerState
	mu              sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(maxFailures int32, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		state:        CircuitBreakerClosed,
	}
}

// IsOpen returns true if the circuit breaker is open
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	if cb.state == CircuitBreakerOpen {
		// Check if we should transition to half-open
		if time.Since(time.Unix(0, atomic.LoadInt64(&cb.lastFailureTime))) > cb.resetTimeout {
			cb.state = CircuitBreakerHalfOpen
		}
	}
	
	return cb.state == CircuitBreakerOpen
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	atomic.StoreInt32(&cb.currentFailures, 0)
	if cb.state == CircuitBreakerHalfOpen {
		cb.state = CircuitBreakerClosed
	}
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	failures := atomic.AddInt32(&cb.currentFailures, 1)
	atomic.StoreInt64(&cb.lastFailureTime, time.Now().UnixNano())
	
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	if failures >= cb.maxFailures {
		cb.state = CircuitBreakerOpen
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// flowControllerImpl implements FlowController interface
type flowControllerImpl struct {
	config          *FlowControlConfig
	circuitBreaker  *CircuitBreaker
	eventBuffer     chan *Event
	throughputCount int64
	droppedCount    int64
	bufferedCount   int64
	lastThroughputReset int64
	mu              sync.RWMutex
	running         bool
	stopCh          chan struct{}
}

// NewFlowController creates a new flow controller
func NewFlowController(config *FlowControlConfig) FlowController {
	if config == nil {
		config = &FlowControlConfig{
			Strategy:              BufferStrategy,
			BufferSize:            10000,
			BlockTimeout:          5 * time.Second,
			MaxThroughput:         50000, // events per second
			CircuitBreakerEnabled: true,
		}
	}
	
	var circuitBreaker *CircuitBreaker
	if config.CircuitBreakerEnabled {
		circuitBreaker = NewCircuitBreaker(100, 30*time.Second) // 100 failures, 30s reset
	}
	
	var eventBuffer chan *Event
	if config.Strategy == BufferStrategy && config.BufferSize > 0 {
		eventBuffer = make(chan *Event, config.BufferSize)
	}
	
	return &flowControllerImpl{
		config:              config,
		circuitBreaker:      circuitBreaker,
		eventBuffer:         eventBuffer,
		lastThroughputReset: time.Now().Unix(),
		stopCh:              make(chan struct{}),
	}
}

// CanProcess checks if we can process more events
func (fc *flowControllerImpl) CanProcess() bool {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	
	// Check circuit breaker
	if fc.circuitBreaker != nil && fc.circuitBreaker.IsOpen() {
		return false
	}
	
	// Check throughput limits
	if fc.config.MaxThroughput > 0 {
		currentThroughput := fc.GetCurrentThroughput()
		if currentThroughput > fc.config.MaxThroughput {
			return false
		}
	}
	
	// Check buffer capacity
	if fc.eventBuffer != nil {
		return len(fc.eventBuffer) < cap(fc.eventBuffer)
	}
	
	return true
}

// OnEventProcessed records a successful event processing
func (fc *flowControllerImpl) OnEventProcessed() {
	atomic.AddInt64(&fc.throughputCount, 1)
	
	if fc.circuitBreaker != nil {
		fc.circuitBreaker.RecordSuccess()
	}
}

// OnEventDropped records a dropped event
func (fc *flowControllerImpl) OnEventDropped() {
	atomic.AddInt64(&fc.droppedCount, 1)
	
	if fc.circuitBreaker != nil {
		fc.circuitBreaker.RecordFailure()
	}
}

// OnEventBuffered records a buffered event
func (fc *flowControllerImpl) OnEventBuffered() {
	atomic.AddInt64(&fc.bufferedCount, 1)
}

// ApplyBackpressure applies backpressure to an event
func (fc *flowControllerImpl) ApplyBackpressure(event *Event) error {
	if !fc.CanProcess() {
		switch fc.config.Strategy {
		case BlockStrategy:
			return fc.blockEvent(event)
		case DropStrategy:
			fc.OnEventDropped()
			return errors.New("event dropped due to backpressure")
		case BufferStrategy:
			return fc.bufferEvent(event)
		default:
			return errors.New("unknown backpressure strategy")
		}
	}
	
	fc.OnEventProcessed()
	return nil
}

// blockEvent blocks until the event can be processed or timeout occurs
func (fc *flowControllerImpl) blockEvent(event *Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), fc.config.BlockTimeout)
	defer cancel()
	
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			fc.OnEventDropped()
			return errors.New("event blocked due to timeout")
		case <-ticker.C:
			if fc.CanProcess() {
				fc.OnEventProcessed()
				return nil
			}
		}
	}
}

// bufferEvent buffers an event if there's capacity
func (fc *flowControllerImpl) bufferEvent(event *Event) error {
	if fc.eventBuffer == nil {
		return errors.New("buffer not configured")
	}
	
	select {
	case fc.eventBuffer <- event:
		fc.OnEventBuffered()
		return nil
	default:
		fc.OnEventDropped()
		return errors.New("buffer full, event dropped")
	}
}

// GetCurrentThroughput returns the current throughput in events per second
func (fc *flowControllerImpl) GetCurrentThroughput() float64 {
	now := time.Now().Unix()
	lastReset := atomic.LoadInt64(&fc.lastThroughputReset)
	
	if now > lastReset {
		// Reset throughput counter every second
		if atomic.CompareAndSwapInt64(&fc.lastThroughputReset, lastReset, now) {
			atomic.StoreInt64(&fc.throughputCount, 0)
			return 0
		}
	}
	
	count := atomic.LoadInt64(&fc.throughputCount)
	duration := now - lastReset
	if duration <= 0 {
		duration = 1
	}
	
	return float64(count) / float64(duration)
}

// GetBufferUtilization returns the buffer utilization as a percentage
func (fc *flowControllerImpl) GetBufferUtilization() float64 {
	if fc.eventBuffer == nil {
		return 0.0
	}
	
	return float64(len(fc.eventBuffer)) / float64(cap(fc.eventBuffer)) * 100.0
}

// IsCircuitOpen returns true if the circuit breaker is open
func (fc *flowControllerImpl) IsCircuitOpen() bool {
	if fc.circuitBreaker == nil {
		return false
	}
	return fc.circuitBreaker.IsOpen()
}

// RecordSuccess records a successful operation
func (fc *flowControllerImpl) RecordSuccess() {
	if fc.circuitBreaker != nil {
		fc.circuitBreaker.RecordSuccess()
	}
}

// RecordFailure records a failed operation
func (fc *flowControllerImpl) RecordFailure() {
	if fc.circuitBreaker != nil {
		fc.circuitBreaker.RecordFailure()
	}
}

// UpdateConfig updates the flow control configuration
func (fc *flowControllerImpl) UpdateConfig(config *FlowControlConfig) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	
	fc.config = config
	
	// Recreate circuit breaker if needed
	if config.CircuitBreakerEnabled && fc.circuitBreaker == nil {
		fc.circuitBreaker = NewCircuitBreaker(100, 30*time.Second)
	} else if !config.CircuitBreakerEnabled {
		fc.circuitBreaker = nil
	}
	
	// Recreate buffer if needed
	if config.Strategy == BufferStrategy && config.BufferSize > 0 && fc.eventBuffer == nil {
		fc.eventBuffer = make(chan *Event, config.BufferSize)
	}
	
	return nil
}

// GetConfig returns the current configuration
func (fc *flowControllerImpl) GetConfig() *FlowControlConfig {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	
	// Return a copy
	return &FlowControlConfig{
		Strategy:              fc.config.Strategy,
		BufferSize:            fc.config.BufferSize,
		BlockTimeout:          fc.config.BlockTimeout,
		MaxThroughput:         fc.config.MaxThroughput,
		CircuitBreakerEnabled: fc.config.CircuitBreakerEnabled,
	}
}

// Start starts the flow controller
func (fc *flowControllerImpl) Start(ctx context.Context) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	
	if fc.running {
		return nil
	}
	
	fc.running = true
	fc.stopCh = make(chan struct{})
	
	// Start monitoring goroutines
	go fc.monitoringLoop(ctx)
	if fc.eventBuffer != nil {
		go fc.bufferProcessingLoop(ctx)
	}
	
	return nil
}

// Stop stops the flow controller
func (fc *flowControllerImpl) Stop() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	
	if !fc.running {
		return nil
	}
	
	fc.running = false
	close(fc.stopCh)
	
	if fc.eventBuffer != nil {
		close(fc.eventBuffer)
	}
	
	return nil
}

// monitoringLoop monitors flow control metrics
func (fc *flowControllerImpl) monitoringLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-fc.stopCh:
			return
		case <-ticker.C:
			// Log metrics or update monitoring systems
			// This could integrate with Prometheus or other monitoring
			
			throughput := fc.GetCurrentThroughput()
			bufferUtil := fc.GetBufferUtilization()
			droppedCount := atomic.LoadInt64(&fc.droppedCount)
			
			_ = throughput // Use metrics as needed
			_ = bufferUtil
			_ = droppedCount
		}
	}
}

// bufferProcessingLoop processes buffered events
func (fc *flowControllerImpl) bufferProcessingLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-fc.stopCh:
			return
		case event, ok := <-fc.eventBuffer:
			if !ok {
				return // Buffer closed
			}
			
			// Process the buffered event
			// This would integrate with the main event processing pipeline
			_ = event
			fc.OnEventProcessed()
		}
	}
}

// FlowControlMetrics holds metrics for flow control
type FlowControlMetrics struct {
	ThroughputPerSec    float64 `json:"throughput_per_sec"`
	BufferUtilization   float64 `json:"buffer_utilization"`
	EventsDropped       int64   `json:"events_dropped"`
	EventsBuffered      int64   `json:"events_buffered"`
	CircuitBreakerState string  `json:"circuit_breaker_state"`
}

// GetMetrics returns current flow control metrics
func (fc *flowControllerImpl) GetMetrics() *FlowControlMetrics {
	metrics := &FlowControlMetrics{
		ThroughputPerSec:  fc.GetCurrentThroughput(),
		BufferUtilization: fc.GetBufferUtilization(),
		EventsDropped:     atomic.LoadInt64(&fc.droppedCount),
		EventsBuffered:    atomic.LoadInt64(&fc.bufferedCount),
	}
	
	if fc.circuitBreaker != nil {
		switch fc.circuitBreaker.GetState() {
		case CircuitBreakerClosed:
			metrics.CircuitBreakerState = "closed"
		case CircuitBreakerOpen:
			metrics.CircuitBreakerState = "open"
		case CircuitBreakerHalfOpen:
			metrics.CircuitBreakerState = "half-open"
		}
	} else {
		metrics.CircuitBreakerState = "disabled"
	}
	
	return metrics
}

// AdaptiveFlowController implements adaptive flow control based on system load
type AdaptiveFlowController struct {
	baseController FlowController
	loadMonitor    *LoadMonitor
	adaptation     *AdaptationStrategy
	mu             sync.RWMutex
}

// LoadMonitor tracks system load metrics
type LoadMonitor struct {
	cpuUsage    float64
	memoryUsage float64
	lastUpdate  time.Time
	mu          sync.RWMutex
}

// AdaptationStrategy defines how to adapt flow control based on load
type AdaptationStrategy struct {
	CPUThreshold    float64 `json:"cpu_threshold"`
	MemoryThreshold float64 `json:"memory_threshold"`
	ScaleFactor     float64 `json:"scale_factor"`
}

// NewAdaptiveFlowController creates an adaptive flow controller
func NewAdaptiveFlowController(baseController FlowController) *AdaptiveFlowController {
	return &AdaptiveFlowController{
		baseController: baseController,
		loadMonitor:    &LoadMonitor{},
		adaptation: &AdaptationStrategy{
			CPUThreshold:    0.8,  // 80% CPU
			MemoryThreshold: 0.8,  // 80% Memory
			ScaleFactor:     0.5,  // Scale down to 50%
		},
	}
}

// CanProcess checks if we can process events based on adaptive logic
func (afc *AdaptiveFlowController) CanProcess() bool {
	afc.mu.RLock()
	defer afc.mu.RUnlock()
	
	// Check base controller first
	if !afc.baseController.CanProcess() {
		return false
	}
	
	// Check system load
	afc.loadMonitor.mu.RLock()
	cpuUsage := afc.loadMonitor.cpuUsage
	memoryUsage := afc.loadMonitor.memoryUsage
	afc.loadMonitor.mu.RUnlock()
	
	// Apply adaptive logic
	if cpuUsage > afc.adaptation.CPUThreshold || memoryUsage > afc.adaptation.MemoryThreshold {
		// System under load, apply backpressure more aggressively
		return false
	}
	
	return true
}

// UpdateSystemLoad updates the current system load metrics
func (afc *AdaptiveFlowController) UpdateSystemLoad(cpuUsage, memoryUsage float64) {
	afc.loadMonitor.mu.Lock()
	defer afc.loadMonitor.mu.Unlock()
	
	afc.loadMonitor.cpuUsage = cpuUsage
	afc.loadMonitor.memoryUsage = memoryUsage
	afc.loadMonitor.lastUpdate = time.Now()
}

// Delegate other methods to the base controller
func (afc *AdaptiveFlowController) OnEventProcessed() {
	afc.baseController.OnEventProcessed()
}

func (afc *AdaptiveFlowController) OnEventDropped() {
	afc.baseController.OnEventDropped()
}

func (afc *AdaptiveFlowController) OnEventBuffered() {
	afc.baseController.OnEventBuffered()
}

func (afc *AdaptiveFlowController) ApplyBackpressure(event *Event) error {
	return afc.baseController.ApplyBackpressure(event)
}

func (afc *AdaptiveFlowController) GetCurrentThroughput() float64 {
	return afc.baseController.GetCurrentThroughput()
}

func (afc *AdaptiveFlowController) GetBufferUtilization() float64 {
	return afc.baseController.GetBufferUtilization()
}

func (afc *AdaptiveFlowController) IsCircuitOpen() bool {
	return afc.baseController.IsCircuitOpen()
}

func (afc *AdaptiveFlowController) RecordSuccess() {
	afc.baseController.RecordSuccess()
}

func (afc *AdaptiveFlowController) RecordFailure() {
	afc.baseController.RecordFailure()
}

func (afc *AdaptiveFlowController) UpdateConfig(config *FlowControlConfig) error {
	return afc.baseController.UpdateConfig(config)
}

func (afc *AdaptiveFlowController) GetConfig() *FlowControlConfig {
	return afc.baseController.GetConfig()
}

func (afc *AdaptiveFlowController) Start(ctx context.Context) error {
	return afc.baseController.Start(ctx)
}

func (afc *AdaptiveFlowController) Stop() error {
	return afc.baseController.Stop()
}