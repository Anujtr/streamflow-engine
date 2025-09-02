package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// pebbleStateStore implements the StateStore interface using Pebble database
type pebbleStateStore struct {
	db       *pebble.DB
	dataDir  string
	mu       sync.RWMutex
	closed   bool
}

// NewPebbleStateStore creates a new Pebble-based state store
func NewPebbleStateStore(dataDir string) (*pebbleStateStore, error) {
	dbPath := filepath.Join(dataDir, "stream-state")
	
	// Configure Pebble options for state store
	opts := &pebble.Options{
		Cache:                       pebble.NewCache(64 << 20), // 64MB cache
		MemTableSize:                32 << 20,                  // 32MB memtable
		MemTableStopWritesThreshold: 4,
		MaxConcurrentCompactions:    func() int { return 4 },
		L0StopWritesThreshold:       24,
		L0CompactionThreshold:       4,
		LBaseMaxBytes:               256 << 20, // 256MB
	}
	
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open Pebble state store: %v", err)
	}
	
	return &pebbleStateStore{
		db:      db,
		dataDir: dataDir,
	}, nil
}

// Get retrieves a value by key from the state store
func (s *pebbleStateStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.closed {
		return nil, fmt.Errorf("state store is closed")
	}
	
	value, closer, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get value: %v", err)
	}
	defer closer.Close()
	
	// Make a copy since the slice is only valid until closer is called
	result := make([]byte, len(value))
	copy(result, value)
	
	return result, nil
}

// Put stores a key-value pair in the state store
func (s *pebbleStateStore) Put(ctx context.Context, key string, value []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.closed {
		return fmt.Errorf("state store is closed")
	}
	
	return s.db.Set([]byte(key), value, pebble.Sync)
}

// Delete removes a key from the state store
func (s *pebbleStateStore) Delete(ctx context.Context, key string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.closed {
		return fmt.Errorf("state store is closed")
	}
	
	return s.db.Delete([]byte(key), pebble.Sync)
}

// GetRange retrieves all key-value pairs within a range
func (s *pebbleStateStore) GetRange(ctx context.Context, startKey, endKey string) (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.closed {
		return nil, fmt.Errorf("state store is closed")
	}
	
	result := make(map[string][]byte)
	
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(startKey),
		UpperBound: []byte(endKey),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %v", err)
	}
	defer iter.Close()
	
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())
		result[key] = value
	}
	
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}
	
	return result, nil
}

// GetWindowState retrieves state for a specific window and key
func (s *pebbleStateStore) GetWindowState(ctx context.Context, window *Window, key string) ([]byte, error) {
	windowKey := s.makeWindowStateKey(window, key)
	return s.Get(ctx, windowKey)
}

// PutWindowState stores state for a specific window and key
func (s *pebbleStateStore) PutWindowState(ctx context.Context, window *Window, key string, value []byte) error {
	windowKey := s.makeWindowStateKey(window, key)
	return s.Put(ctx, windowKey, value)
}

// DeleteWindowState removes state for a specific window and key
func (s *pebbleStateStore) DeleteWindowState(ctx context.Context, window *Window, key string) error {
	windowKey := s.makeWindowStateKey(window, key)
	return s.Delete(ctx, windowKey)
}

// ExpireWindow removes all state for a specific window
func (s *pebbleStateStore) ExpireWindow(ctx context.Context, window *Window) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.closed {
		return fmt.Errorf("state store is closed")
	}
	
	windowPrefix := s.makeWindowPrefix(window)
	
	batch := s.db.NewBatch()
	defer batch.Close()
	
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(windowPrefix),
		UpperBound: []byte(windowPrefix + "~"), // ASCII ~ is greater than all other chars
	})
	if err != nil {
		return fmt.Errorf("failed to create iterator: %v", err)
	}
	defer iter.Close()
	
	keysToDelete := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return fmt.Errorf("failed to add delete to batch: %v", err)
		}
		keysToDelete++
	}
	
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %v", err)
	}
	
	if keysToDelete > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("failed to commit batch deletion: %v", err)
		}
	}
	
	return nil
}

// ListActiveWindows returns a list of all windows that have state
func (s *pebbleStateStore) ListActiveWindows(ctx context.Context) ([]*Window, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.closed {
		return nil, fmt.Errorf("state store is closed")
	}
	
	windowSet := make(map[string]*Window)
	
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("window:"),
		UpperBound: []byte("window:~"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %v", err)
	}
	defer iter.Close()
	
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if strings.HasPrefix(key, "window:") {
			// Parse window from key format: "window:{start}:{end}:{userkey}"
			parts := strings.Split(key, ":")
			if len(parts) >= 4 {
				windowKey := strings.Join(parts[1:3], ":")
				if _, exists := windowSet[windowKey]; !exists {
					start, _ := time.Parse(time.RFC3339, parts[1])
					end, _ := time.Parse(time.RFC3339, parts[2])
					windowSet[windowKey] = &Window{
						Start: start,
						End:   end,
					}
				}
			}
		}
	}
	
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}
	
	windows := make([]*Window, 0, len(windowSet))
	for _, window := range windowSet {
		windows = append(windows, window)
	}
	
	return windows, nil
}

// Close closes the state store
func (s *pebbleStateStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.closed {
		return nil
	}
	
	s.closed = true
	return s.db.Close()
}

// Flush flushes any pending writes to disk
func (s *pebbleStateStore) Flush() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.closed {
		return fmt.Errorf("state store is closed")
	}
	
	return s.db.Flush()
}

// makeWindowStateKey creates a key for window state storage
func (s *pebbleStateStore) makeWindowStateKey(window *Window, key string) string {
	return fmt.Sprintf("window:%s:%s:%s", 
		window.Start.Format(time.RFC3339), 
		window.End.Format(time.RFC3339), 
		key)
}

// makeWindowPrefix creates a prefix for all keys in a window
func (s *pebbleStateStore) makeWindowPrefix(window *Window) string {
	return fmt.Sprintf("window:%s:%s:", 
		window.Start.Format(time.RFC3339), 
		window.End.Format(time.RFC3339))
}

// memoryStateStore implements the StateStore interface using in-memory storage
type memoryStateStore struct {
	data      map[string][]byte
	windows   map[string]map[string][]byte // window -> key -> value
	mu        sync.RWMutex
}

// NewMemoryStateStore creates a new in-memory state store (for testing/development)
func NewMemoryStateStore() *memoryStateStore {
	return &memoryStateStore{
		data:    make(map[string][]byte),
		windows: make(map[string]map[string][]byte),
	}
}

// Get retrieves a value by key from the memory state store
func (s *memoryStateStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	value, exists := s.data[key]
	if !exists {
		return nil, nil
	}
	
	// Return a copy
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Put stores a key-value pair in the memory state store
func (s *memoryStateStore) Put(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Store a copy
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	s.data[key] = valueCopy
	
	return nil
}

// Delete removes a key from the memory state store
func (s *memoryStateStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	delete(s.data, key)
	return nil
}

// GetRange retrieves all key-value pairs within a range
func (s *memoryStateStore) GetRange(ctx context.Context, startKey, endKey string) (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	result := make(map[string][]byte)
	
	for key, value := range s.data {
		if key >= startKey && key < endKey {
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			result[key] = valueCopy
		}
	}
	
	return result, nil
}

// GetWindowState retrieves state for a specific window and key
func (s *memoryStateStore) GetWindowState(ctx context.Context, window *Window, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	windowKey := s.makeWindowKey(window)
	if windowData, exists := s.windows[windowKey]; exists {
		if value, exists := windowData[key]; exists {
			result := make([]byte, len(value))
			copy(result, value)
			return result, nil
		}
	}
	
	return nil, nil
}

// PutWindowState stores state for a specific window and key
func (s *memoryStateStore) PutWindowState(ctx context.Context, window *Window, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	windowKey := s.makeWindowKey(window)
	if _, exists := s.windows[windowKey]; !exists {
		s.windows[windowKey] = make(map[string][]byte)
	}
	
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	s.windows[windowKey][key] = valueCopy
	
	return nil
}

// DeleteWindowState removes state for a specific window and key
func (s *memoryStateStore) DeleteWindowState(ctx context.Context, window *Window, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	windowKey := s.makeWindowKey(window)
	if windowData, exists := s.windows[windowKey]; exists {
		delete(windowData, key)
		if len(windowData) == 0 {
			delete(s.windows, windowKey)
		}
	}
	
	return nil
}

// ExpireWindow removes all state for a specific window
func (s *memoryStateStore) ExpireWindow(ctx context.Context, window *Window) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	windowKey := s.makeWindowKey(window)
	delete(s.windows, windowKey)
	
	return nil
}

// ListActiveWindows returns a list of all windows that have state
func (s *memoryStateStore) ListActiveWindows(ctx context.Context) ([]*Window, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	windows := make([]*Window, 0, len(s.windows))
	for windowKey := range s.windows {
		// Parse window from key format: "{start}:{end}"
		// Need to be careful because RFC3339 contains colons, so find the middle split point
		// Look for the pattern where we have "Z:" which indicates the end of the first timestamp
		splitIndex := strings.Index(windowKey, "Z:")
		if splitIndex != -1 {
			startStr := windowKey[:splitIndex+1]  // Include the Z
			endStr := windowKey[splitIndex+2:]    // Skip the Z:
			
			start, startErr := time.Parse(time.RFC3339, startStr)
			end, endErr := time.Parse(time.RFC3339, endStr)
			
			if startErr == nil && endErr == nil {
				windows = append(windows, &Window{
					Start: start,
					End:   end,
				})
			}
		}
	}
	
	return windows, nil
}

// Close closes the memory state store (no-op)
func (s *memoryStateStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.data = make(map[string][]byte)
	s.windows = make(map[string]map[string][]byte)
	
	return nil
}

// Flush flushes any pending writes (no-op for memory store)
func (s *memoryStateStore) Flush() error {
	return nil
}

// makeWindowKey creates a key for window identification
func (s *memoryStateStore) makeWindowKey(window *Window) string {
	return fmt.Sprintf("%s:%s", 
		window.Start.Format(time.RFC3339), 
		window.End.Format(time.RFC3339))
}

// WindowState represents aggregated state within a window
type WindowState struct {
	Key       string                 `json:"key"`
	Window    *Window                `json:"window"`
	Count     int64                  `json:"count"`
	Sum       float64                `json:"sum"`
	Min       float64                `json:"min"`
	Max       float64                `json:"max"`
	Events    []*Event               `json:"events,omitempty"` // For debugging/small windows
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	UpdatedAt time.Time              `json:"updated_at"`
}

// ToJSON converts the window state to JSON
func (ws *WindowState) ToJSON() ([]byte, error) {
	return json.Marshal(ws)
}

// FromJSON parses window state from JSON
func (ws *WindowState) FromJSON(data []byte) error {
	return json.Unmarshal(data, ws)
}

// Update updates the window state with a new event
func (ws *WindowState) Update(event *Event, valueExtractor ValueExtractorFunc) error {
	ws.Count++
	ws.UpdatedAt = time.Now()
	
	if valueExtractor != nil {
		value, err := valueExtractor(event)
		if err != nil {
			return err
		}
		
		ws.Sum += value
		
		if ws.Count == 1 {
			ws.Min = value
			ws.Max = value
		} else {
			if value < ws.Min {
				ws.Min = value
			}
			if value > ws.Max {
				ws.Max = value
			}
		}
	}
	
	// Store events for small windows (debugging/testing)
	if ws.Count <= 100 {
		if ws.Events == nil {
			ws.Events = make([]*Event, 0)
		}
		ws.Events = append(ws.Events, event.Clone())
	}
	
	return nil
}

// GetAverage returns the average value
func (ws *WindowState) GetAverage() float64 {
	if ws.Count == 0 {
		return 0
	}
	return ws.Sum / float64(ws.Count)
}