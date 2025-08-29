package storage

import (
	"fmt"
	"sync"

	"github.com/Anujtr/streamflow-engine/internal/persistence"
)

type Storage struct {
	topics          map[string]*Topic
	mu              sync.RWMutex
	pebbleStorage   *persistence.PebbleStorage
	offsetStore     *persistence.OffsetStore
	persistenceMode bool
}

// StorageConfig holds configuration for storage
type StorageConfig struct {
	DataDir         string `json:"data_dir"`
	PersistenceMode bool   `json:"persistence_mode"`
}

func NewStorage() *Storage {
	return &Storage{
		topics:          make(map[string]*Topic),
		persistenceMode: false,
	}
}

// NewPersistentStorage creates a new storage instance with Pebble persistence
func NewPersistentStorage(config StorageConfig) (*Storage, error) {
	pebbleStorage, err := persistence.NewPebbleStorage(config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pebble storage: %v", err)
	}

	// Create offset store using the same Pebble instance
	offsetStore := persistence.NewOffsetStore(pebbleStorage.GetDB())

	return &Storage{
		topics:          make(map[string]*Topic),
		pebbleStorage:   pebbleStorage,
		offsetStore:     offsetStore,
		persistenceMode: true,
	}, nil
}

func (s *Storage) CreateTopic(name string, numPartitions int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	if s.persistenceMode && s.pebbleStorage != nil {
		s.topics[name] = NewPersistentTopic(name, numPartitions, s.pebbleStorage)
	} else {
		s.topics[name] = NewTopic(name, numPartitions)
	}
	
	return nil
}

func (s *Storage) GetTopic(name string) (*Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topic, exists := s.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", name)
	}

	return topic, nil
}

func (s *Storage) ListTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]string, 0, len(s.topics))
	for name := range s.topics {
		topics = append(topics, name)
	}
	return topics
}

func (s *Storage) Produce(topicName string, msg *Message) (int32, int64, error) {
	topic, err := s.GetTopic(topicName)
	if err != nil {
		return -1, -1, err
	}

	return topic.Produce(msg)
}

func (s *Storage) Consume(topicName string, partition int32, offset int64, maxMessages int32) ([]*Message, bool, error) {
	topic, err := s.GetTopic(topicName)
	if err != nil {
		return nil, false, err
	}

	return topic.Consume(partition, offset, maxMessages)
}

func (s *Storage) GetTopicInfo(topicName string) (map[int32]PartitionInfo, error) {
	topic, err := s.GetTopic(topicName)
	if err != nil {
		return nil, err
	}

	return topic.GetPartitionInfo(), nil
}

// GetOffsetStore returns the offset store for consumer group offset management
func (s *Storage) GetOffsetStore() *persistence.OffsetStore {
	return s.offsetStore
}

// Close closes the storage and cleans up resources
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pebbleStorage != nil {
		return s.pebbleStorage.Close()
	}

	return nil
}