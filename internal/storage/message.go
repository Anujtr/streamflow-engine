package storage

import (
	"sync"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Message struct {
	Key       string
	Value     []byte
	Partition int32
	Offset    int64
	Timestamp time.Time
}

func (m *Message) ToProto() *pb.Message {
	return &pb.Message{
		Key:       m.Key,
		Value:     m.Value,
		Partition: m.Partition,
		Offset:    m.Offset,
		Timestamp: timestamppb.New(m.Timestamp),
	}
}

func FromProto(msg *pb.Message) *Message {
	return &Message{
		Key:       msg.Key,
		Value:     msg.Value,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp.AsTime(),
	}
}

type Partition struct {
	ID       int32
	Messages []*Message
	NextOffset int64
	mu       sync.RWMutex
}

func NewPartition(id int32) *Partition {
	return &Partition{
		ID:       id,
		Messages: make([]*Message, 0),
		NextOffset: 0,
	}
}

func (p *Partition) Append(msg *Message) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	msg.Partition = p.ID
	msg.Offset = p.NextOffset
	msg.Timestamp = time.Now()
	
	p.Messages = append(p.Messages, msg)
	p.NextOffset++
	
	return msg.Offset
}

func (p *Partition) Read(offset int64, maxMessages int32) ([]*Message, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset < 0 || offset >= int64(len(p.Messages)) {
		return nil, false
	}

	end := offset + int64(maxMessages)
	if end > int64(len(p.Messages)) {
		end = int64(len(p.Messages))
	}

	messages := make([]*Message, end-offset)
	copy(messages, p.Messages[offset:end])

	hasMore := end < int64(len(p.Messages))
	return messages, hasMore
}

func (p *Partition) GetNextOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.NextOffset
}

func (p *Partition) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.Messages)
}

func (p *Partition) GetID() int32 {
	return p.ID
}