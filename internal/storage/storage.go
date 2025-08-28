package storage

import (
	"fmt"
	"sync"
)

type Storage struct {
	topics map[string]*Topic
	mu     sync.RWMutex
}

func NewStorage() *Storage {
	return &Storage{
		topics: make(map[string]*Topic),
	}
}

func (s *Storage) CreateTopic(name string, numPartitions int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	s.topics[name] = NewTopic(name, numPartitions)
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