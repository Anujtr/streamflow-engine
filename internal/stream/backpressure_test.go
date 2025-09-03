package stream

import (
	"context"
	"testing"
	"time"
)

func TestNewCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(5, 30*time.Second)
	if cb == nil {
		t.Fatal("NewCircuitBreaker returned nil")
	}

	if cb.maxFailures != 5 {
		t.Errorf("Expected max failures 5, got %d", cb.maxFailures)
	}

	if cb.resetTimeout != 30*time.Second {
		t.Errorf("Expected reset timeout 30s, got %v", cb.resetTimeout)
	}

	if cb.GetState() != CircuitBreakerClosed {
		t.Errorf("Expected initial state closed, got %v", cb.GetState())
	}
}

func TestCircuitBreaker_RecordSuccess(t *testing.T) {
	cb := NewCircuitBreaker(3, 30*time.Second)

	// Record some failures first
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.currentFailures != 2 {
		t.Errorf("Expected 2 failures, got %d", cb.currentFailures)
	}

	// Record success should reset failures
	cb.RecordSuccess()

	if cb.currentFailures != 0 {
		t.Errorf("Expected 0 failures after success, got %d", cb.currentFailures)
	}

	if cb.GetState() != CircuitBreakerClosed {
		t.Errorf("Expected state closed after success, got %v", cb.GetState())
	}
}

func TestCircuitBreaker_RecordFailure(t *testing.T) {
	cb := NewCircuitBreaker(3, 30*time.Second)

	// Circuit should be closed initially
	if cb.IsOpen() {
		t.Error("Circuit should be closed initially")
	}

	// Record failures up to threshold
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.IsOpen() {
		t.Error("Circuit should still be closed before threshold")
	}

	// This failure should open the circuit
	cb.RecordFailure()

	if !cb.IsOpen() {
		t.Error("Circuit should be open after reaching threshold")
	}

	if cb.GetState() != CircuitBreakerOpen {
		t.Errorf("Expected state open, got %v", cb.GetState())
	}
}

func TestCircuitBreaker_HalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(2, 100*time.Millisecond) // Short timeout for testing

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if !cb.IsOpen() {
		t.Fatal("Circuit should be open")
	}

	// Wait for reset timeout
	time.Sleep(150 * time.Millisecond)

	// Circuit should transition to half-open
	if !cb.IsOpen() {
		// IsOpen() should return false for half-open state after timeout
		// But let's check the actual state
		if cb.GetState() != CircuitBreakerHalfOpen {
			t.Errorf("Expected state half-open after timeout, got %v", cb.GetState())
		}
	}

	// Success in half-open should close circuit
	cb.RecordSuccess()
	if cb.GetState() != CircuitBreakerClosed {
		t.Errorf("Expected state closed after success in half-open, got %v", cb.GetState())
	}
}

func TestNewFlowController(t *testing.T) {
	config := &FlowControlConfig{
		Strategy:              BufferStrategy,
		BufferSize:            1000,
		BlockTimeout:          5 * time.Second,
		MaxThroughput:         1000,
		CircuitBreakerEnabled: true,
	}

	fc := NewFlowController(config)
	if fc == nil {
		t.Fatal("NewFlowController returned nil")
	}

	retrievedConfig := fc.GetConfig()
	if retrievedConfig.Strategy != config.Strategy {
		t.Errorf("Expected strategy %v, got %v", config.Strategy, retrievedConfig.Strategy)
	}
	if retrievedConfig.BufferSize != config.BufferSize {
		t.Errorf("Expected buffer size %d, got %d", config.BufferSize, retrievedConfig.BufferSize)
	}
}

func TestNewFlowController_DefaultConfig(t *testing.T) {
	fc := NewFlowController(nil)
	if fc == nil {
		t.Fatal("NewFlowController with nil config returned nil")
	}

	config := fc.GetConfig()
	if config.Strategy != BufferStrategy {
		t.Errorf("Expected default strategy %v, got %v", BufferStrategy, config.Strategy)
	}
	if config.BufferSize != 10000 {
		t.Errorf("Expected default buffer size 10000, got %d", config.BufferSize)
	}
}

func TestFlowController_CanProcess(t *testing.T) {
	config := &FlowControlConfig{
		Strategy:              BufferStrategy,
		BufferSize:            2,
		MaxThroughput:         0, // No throughput limit
		CircuitBreakerEnabled: false,
	}

	fc := NewFlowController(config)

	// Should be able to process initially
	if !fc.CanProcess() {
		t.Error("Should be able to process initially")
	}
}

func TestFlowController_OnEventProcessed(t *testing.T) {
	fc := NewFlowController(nil)

	initialThroughput := fc.GetCurrentThroughput()

	fc.OnEventProcessed()

	// Throughput should increase (though may be 0 due to timing)
	newThroughput := fc.GetCurrentThroughput()
	if newThroughput < initialThroughput {
		t.Errorf("Throughput should not decrease: was %f, now %f", initialThroughput, newThroughput)
	}
}

func TestFlowController_OnEventDropped(t *testing.T) {
	config := &FlowControlConfig{
		Strategy:              DropStrategy,
		CircuitBreakerEnabled: true,
	}

	fc := NewFlowController(config)

	// Record some drops
	fc.OnEventDropped()
	fc.OnEventDropped()

	// This should affect circuit breaker if enabled
	if !fc.IsCircuitOpen() {
		// Circuit might not be open yet, depends on circuit breaker threshold
		// Just verify the method doesn't panic
	}
}

func TestFlowController_ApplyBackpressure_DropStrategy(t *testing.T) {
	config := &FlowControlConfig{
		Strategy:              DropStrategy,
		BufferSize:            0,
		MaxThroughput:         1, // Very low throughput to trigger backpressure
		CircuitBreakerEnabled: true, // Enable circuit breaker
	}

	fc := NewFlowController(config)

	// Force circuit breaker to open by recording failures
	fcImpl := fc.(*flowControllerImpl)
	if fcImpl.circuitBreaker != nil {
		for i := 0; i < 100; i++ {
			fcImpl.circuitBreaker.RecordFailure()
		}
	}

	event := &Event{Key: "test"}
	err := fc.ApplyBackpressure(event)

	// Should drop the event and return error
	if err == nil {
		t.Error("Expected error from drop strategy when backpressure applied")
	}
}

func TestFlowController_ApplyBackpressure_BlockStrategy(t *testing.T) {
	config := &FlowControlConfig{
		Strategy:              BlockStrategy,
		BlockTimeout:          100 * time.Millisecond,
		MaxThroughput:         1, // Very low throughput
		CircuitBreakerEnabled: true, // Enable circuit breaker
	}

	fc := NewFlowController(config)

	// Force circuit breaker to open by recording failures
	fcImpl := fc.(*flowControllerImpl)
	if fcImpl.circuitBreaker != nil {
		for i := 0; i < 100; i++ {
			fcImpl.circuitBreaker.RecordFailure()
		}
	}

	event := &Event{Key: "test"}
	start := time.Now()
	err := fc.ApplyBackpressure(event)
	duration := time.Since(start)

	// Should block for timeout duration
	if err == nil {
		t.Error("Expected error from block strategy timeout")
	}

	if duration < config.BlockTimeout {
		t.Errorf("Expected to block for at least %v, blocked for %v", config.BlockTimeout, duration)
	}
}

func TestFlowController_GetCurrentThroughput(t *testing.T) {
	fc := NewFlowController(nil)

	// Initially should be 0
	throughput := fc.GetCurrentThroughput()
	if throughput != 0 {
		t.Errorf("Expected initial throughput 0, got %f", throughput)
	}

	// Process some events
	fc.OnEventProcessed()
	fc.OnEventProcessed()

	throughput = fc.GetCurrentThroughput()
	// Throughput calculation depends on timing, just verify it's non-negative
	if throughput < 0 {
		t.Errorf("Throughput should be non-negative, got %f", throughput)
	}
}

func TestFlowController_GetBufferUtilization(t *testing.T) {
	config := &FlowControlConfig{
		Strategy:   BufferStrategy,
		BufferSize: 10,
	}

	fc := NewFlowController(config)

	// Initially should be 0%
	utilization := fc.GetBufferUtilization()
	if utilization != 0 {
		t.Errorf("Expected initial buffer utilization 0%%, got %f%%", utilization)
	}
}

func TestFlowController_UpdateConfig(t *testing.T) {
	fc := NewFlowController(nil)

	newConfig := &FlowControlConfig{
		Strategy:              DropStrategy,
		BufferSize:            5000,
		BlockTimeout:          10 * time.Second,
		MaxThroughput:         2000,
		CircuitBreakerEnabled: false,
	}

	err := fc.UpdateConfig(newConfig)
	if err != nil {
		t.Errorf("UpdateConfig should not error: %v", err)
	}

	retrievedConfig := fc.GetConfig()
	if retrievedConfig.Strategy != newConfig.Strategy {
		t.Errorf("Expected strategy %v, got %v", newConfig.Strategy, retrievedConfig.Strategy)
	}
	if retrievedConfig.BufferSize != newConfig.BufferSize {
		t.Errorf("Expected buffer size %d, got %d", newConfig.BufferSize, retrievedConfig.BufferSize)
	}
}

func TestFlowController_StartStop(t *testing.T) {
	fc := NewFlowController(nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the flow controller
	err := fc.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start flow controller: %v", err)
	}

	// Starting again should be safe
	err = fc.Start(ctx)
	if err != nil {
		t.Errorf("Starting already running flow controller should not error: %v", err)
	}

	// Stop the flow controller
	err = fc.Stop()
	if err != nil {
		t.Fatalf("Failed to stop flow controller: %v", err)
	}

	// Stopping again should be safe
	err = fc.Stop()
	if err != nil {
		t.Errorf("Stopping already stopped flow controller should not error: %v", err)
	}
}

func TestFlowController_IsCircuitOpen(t *testing.T) {
	config := &FlowControlConfig{
		CircuitBreakerEnabled: true,
	}

	fc := NewFlowController(config)

	// Initially circuit should be closed
	if fc.IsCircuitOpen() {
		t.Error("Circuit should be closed initially")
	}

	// Record many failures
	for i := 0; i < 200; i++ {
		fc.RecordFailure()
	}

	// Circuit should be open now
	if !fc.IsCircuitOpen() {
		t.Error("Circuit should be open after many failures")
	}
}

func TestFlowController_RecordSuccessFailure(t *testing.T) {
	config := &FlowControlConfig{
		CircuitBreakerEnabled: true,
	}

	fc := NewFlowController(config)

	// Record success and failure
	fc.RecordSuccess()
	fc.RecordFailure()

	// Should not panic or error
	if fc.IsCircuitOpen() {
		// Might or might not be open depending on circuit breaker config
		// Just verify methods work
	}
}

func TestFlowControllerMetrics(t *testing.T) {
	fcImpl := &flowControllerImpl{
		config: &FlowControlConfig{
			Strategy:              BufferStrategy,
			BufferSize:            10,
			CircuitBreakerEnabled: true,
		},
		circuitBreaker: NewCircuitBreaker(5, 30*time.Second),
		eventBuffer:    make(chan *Event, 10),
	}

	metrics := fcImpl.GetMetrics()
	if metrics == nil {
		t.Fatal("GetMetrics returned nil")
	}

	if metrics.CircuitBreakerState == "" {
		t.Error("Circuit breaker state should not be empty")
	}

	if metrics.ThroughputPerSec < 0 {
		t.Errorf("Throughput should be non-negative, got %f", metrics.ThroughputPerSec)
	}

	if metrics.BufferUtilization < 0 || metrics.BufferUtilization > 100 {
		t.Errorf("Buffer utilization should be 0-100%%, got %f", metrics.BufferUtilization)
	}
}

func TestNewAdaptiveFlowController(t *testing.T) {
	baseController := NewFlowController(nil)
	afc := NewAdaptiveFlowController(baseController)

	if afc == nil {
		t.Fatal("NewAdaptiveFlowController returned nil")
	}

	if afc.baseController != baseController {
		t.Error("Base controller not set correctly")
	}
}

func TestAdaptiveFlowController_CanProcess(t *testing.T) {
	baseController := NewFlowController(nil)
	afc := NewAdaptiveFlowController(baseController)

	// Initially should be able to process (no load info)
	if !afc.CanProcess() {
		t.Error("Should be able to process initially")
	}

	// Update with high system load
	afc.UpdateSystemLoad(0.9, 0.9) // 90% CPU, 90% Memory

	// Should not be able to process under high load
	if afc.CanProcess() {
		t.Error("Should not be able to process under high system load")
	}

	// Update with normal load
	afc.UpdateSystemLoad(0.5, 0.5) // 50% CPU, 50% Memory

	// Should be able to process under normal load
	if !afc.CanProcess() {
		t.Error("Should be able to process under normal load")
	}
}

func TestAdaptiveFlowController_UpdateSystemLoad(t *testing.T) {
	baseController := NewFlowController(nil)
	afc := NewAdaptiveFlowController(baseController)

	// Update system load
	afc.UpdateSystemLoad(0.7, 0.8)

	// Verify load is stored
	loadMonitor := afc.loadMonitor
	loadMonitor.mu.RLock()
	cpuUsage := loadMonitor.cpuUsage
	memoryUsage := loadMonitor.memoryUsage
	loadMonitor.mu.RUnlock()

	if cpuUsage != 0.7 {
		t.Errorf("Expected CPU usage 0.7, got %f", cpuUsage)
	}

	if memoryUsage != 0.8 {
		t.Errorf("Expected memory usage 0.8, got %f", memoryUsage)
	}

	if loadMonitor.lastUpdate.IsZero() {
		t.Error("Last update time should be set")
	}
}

func TestAdaptiveFlowController_DelegatedMethods(t *testing.T) {
	baseController := NewFlowController(nil)
	afc := NewAdaptiveFlowController(baseController)

	// Test that methods are properly delegated
	afc.OnEventProcessed()
	afc.OnEventDropped()
	afc.OnEventBuffered()

	throughput := afc.GetCurrentThroughput()
	if throughput < 0 {
		t.Errorf("Throughput should be non-negative, got %f", throughput)
	}

	utilization := afc.GetBufferUtilization()
	if utilization < 0 || utilization > 100 {
		t.Errorf("Buffer utilization should be 0-100%%, got %f", utilization)
	}

	config := afc.GetConfig()
	if config == nil {
		t.Error("GetConfig should return config")
	}

	// Test circuit breaker methods
	afc.RecordSuccess()
	afc.RecordFailure()
	isOpen := afc.IsCircuitOpen()
	_ = isOpen // Just verify no panic

	// Test lifecycle methods
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := afc.Start(ctx)
	if err != nil {
		t.Errorf("Start should not error: %v", err)
	}

	err = afc.Stop()
	if err != nil {
		t.Errorf("Stop should not error: %v", err)
	}
}