package stream

import (
	"testing"
	"time"
)

func TestEvent_Clone(t *testing.T) {
	original := &Event{
		Key:       "test-key",
		Value:     []byte("test-value"),
		Headers:   map[string]string{"header1": "value1", "header2": "value2"},
		Timestamp: time.Now(),
		Partition: 1,
		Offset:    100,
		Topic:     "test-topic",
		Metadata:  map[string]interface{}{"meta1": "value1", "meta2": 42},
	}

	clone := original.Clone()

	// Verify all fields are copied correctly
	if clone.Key != original.Key {
		t.Errorf("Expected key %s, got %s", original.Key, clone.Key)
	}
	if string(clone.Value) != string(original.Value) {
		t.Errorf("Expected value %s, got %s", string(original.Value), string(clone.Value))
	}
	if clone.Partition != original.Partition {
		t.Errorf("Expected partition %d, got %d", original.Partition, clone.Partition)
	}
	if clone.Offset != original.Offset {
		t.Errorf("Expected offset %d, got %d", original.Offset, clone.Offset)
	}
	if clone.Topic != original.Topic {
		t.Errorf("Expected topic %s, got %s", original.Topic, clone.Topic)
	}

	// Verify deep copy - modifications to clone shouldn't affect original
	clone.Value[0] = 'X'
	if original.Value[0] == 'X' {
		t.Error("Value was not deep copied")
	}

	clone.Headers["header1"] = "modified"
	if original.Headers["header1"] == "modified" {
		t.Error("Headers were not deep copied")
	}

	clone.Metadata["meta1"] = "modified"
	if original.Metadata["meta1"] == "modified" {
		t.Error("Metadata was not deep copied")
	}
}

func TestEvent_GetValueAsString(t *testing.T) {
	event := &Event{
		Value: []byte("hello world"),
	}

	result := event.GetValueAsString()
	expected := "hello world"

	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestEvent_SetValueFromJSON(t *testing.T) {
	event := &Event{}
	testData := map[string]interface{}{
		"field1": "value1",
		"field2": 42,
		"field3": true,
	}

	err := event.SetValueFromJSON(testData)
	if err != nil {
		t.Fatalf("Failed to set value from JSON: %v", err)
	}

	// Verify we can get the data back
	var result map[string]interface{}
	err = event.GetValueAsJSON(&result)
	if err != nil {
		t.Fatalf("Failed to get value as JSON: %v", err)
	}

	if result["field1"] != testData["field1"] {
		t.Errorf("Expected field1 %v, got %v", testData["field1"], result["field1"])
	}
	if int(result["field2"].(float64)) != testData["field2"] {
		t.Errorf("Expected field2 %v, got %v", testData["field2"], result["field2"])
	}
	if result["field3"] != testData["field3"] {
		t.Errorf("Expected field3 %v, got %v", testData["field3"], result["field3"])
	}
}

func TestWindow_Contains(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	window := &Window{Start: start, End: end}

	// Test timestamp within window
	withinWindow := start.Add(30 * time.Minute)
	if !window.Contains(withinWindow) {
		t.Error("Window should contain timestamp within bounds")
	}

	// Test timestamp at start (should be included)
	if !window.Contains(start) {
		t.Error("Window should contain start timestamp")
	}

	// Test timestamp at end (should be excluded)
	if window.Contains(end) {
		t.Error("Window should not contain end timestamp")
	}

	// Test timestamp before window
	beforeWindow := start.Add(-time.Minute)
	if window.Contains(beforeWindow) {
		t.Error("Window should not contain timestamp before start")
	}

	// Test timestamp after window
	afterWindow := end.Add(time.Minute)
	if window.Contains(afterWindow) {
		t.Error("Window should not contain timestamp after end")
	}
}

func TestWindow_Duration(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	window := &Window{Start: start, End: end}

	duration := window.Duration()
	expected := 2 * time.Hour

	if duration != expected {
		t.Errorf("Expected duration %v, got %v", expected, duration)
	}
}

func TestWindow_String(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	window := &Window{Start: start, End: end}

	str := window.String()
	expectedStart := start.Format(time.RFC3339)
	expectedEnd := end.Format(time.RFC3339)
	expected := expectedStart + " to " + expectedEnd

	if str != expected {
		t.Errorf("Expected %s, got %s", expected, str)
	}
}

func TestProcessorMetrics_InitialState(t *testing.T) {
	metrics := &ProcessorMetrics{}

	if metrics.EventsProcessed != 0 {
		t.Errorf("Expected EventsProcessed to be 0, got %d", metrics.EventsProcessed)
	}
	if metrics.EventsFiltered != 0 {
		t.Errorf("Expected EventsFiltered to be 0, got %d", metrics.EventsFiltered)
	}
	if metrics.ProcessingErrors != 0 {
		t.Errorf("Expected ProcessingErrors to be 0, got %d", metrics.ProcessingErrors)
	}
	if metrics.AvgLatency != 0 {
		t.Errorf("Expected AvgLatency to be 0, got %v", metrics.AvgLatency)
	}
}

func TestFilterFunc(t *testing.T) {
	event := &Event{
		Topic: "test-topic",
		Key:   "test-key",
	}

	// Test TrueFilter
	if !TrueFilter(event) {
		t.Error("TrueFilter should always return true")
	}

	// Test FilterByTopic
	topicFilter := FilterByTopic("test-topic", "another-topic")
	if !topicFilter(event) {
		t.Error("FilterByTopic should return true for matching topic")
	}

	wrongTopicFilter := FilterByTopic("wrong-topic")
	if wrongTopicFilter(event) {
		t.Error("FilterByTopic should return false for non-matching topic")
	}

	// Test FilterByKeyPrefix
	prefixFilter := FilterByKeyPrefix("test")
	if !prefixFilter(event) {
		t.Error("FilterByKeyPrefix should return true for matching prefix")
	}

	wrongPrefixFilter := FilterByKeyPrefix("wrong")
	if wrongPrefixFilter(event) {
		t.Error("FilterByKeyPrefix should return false for non-matching prefix")
	}
}

func TestMapFunc(t *testing.T) {
	event := &Event{
		Key:      "original-key",
		Value:    []byte("original-value"),
		Headers:  make(map[string]string),
		Metadata: make(map[string]interface{}),
	}

	// Test IdentityMap
	result := IdentityMap(event)
	if result != event {
		t.Error("IdentityMap should return the same event")
	}

	// Test MapAddHeader
	headerMapper := MapAddHeader("test-header", "test-value")
	result = headerMapper(event)
	
	if result == event {
		t.Error("MapAddHeader should return a new event")
	}
	if result.Headers["test-header"] != "test-value" {
		t.Errorf("Expected header value 'test-value', got '%s'", result.Headers["test-header"])
	}

	// Test MapTransformKey
	keyTransformer := MapTransformKey(func(key string) string {
		return key + "-transformed"
	})
	result = keyTransformer(event)
	
	if result.Key != "original-key-transformed" {
		t.Errorf("Expected transformed key 'original-key-transformed', got '%s'", result.Key)
	}

	// Test MapTransformValue
	valueTransformer := MapTransformValue(func(value string) string {
		return value + "-transformed"
	})
	result = valueTransformer(event)
	
	if string(result.Value) != "original-value-transformed" {
		t.Errorf("Expected transformed value 'original-value-transformed', got '%s'", string(result.Value))
	}
}

func TestKeyExtractorFunc(t *testing.T) {
	event := &Event{
		Key:   "test-key",
		Topic: "test-topic",
	}

	// Test KeyByValue
	key := KeyByValue(event)
	if key != "test-key" {
		t.Errorf("Expected key 'test-key', got '%s'", key)
	}

	// Test KeyByTopic
	key = KeyByTopic(event)
	if key != "test-topic" {
		t.Errorf("Expected key 'test-topic', got '%s'", key)
	}
}

func TestValueExtractorFunc(t *testing.T) {
	// Test ValueAsFloat64 with valid JSON number
	event := &Event{}
	err := event.SetValueFromJSON(42.5)
	if err != nil {
		t.Fatalf("Failed to set JSON value: %v", err)
	}

	value, err := ValueAsFloat64(event)
	if err != nil {
		t.Fatalf("Failed to extract float64 value: %v", err)
	}
	if value != 42.5 {
		t.Errorf("Expected value 42.5, got %f", value)
	}

	// Test ValueAsFloat64 with invalid JSON
	event.Value = []byte("invalid json")
	_, err = ValueAsFloat64(event)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}