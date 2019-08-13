package storage

import (
	"github.com/dmitryikh/tube/message"
)

type Storage interface {
	AddTopic(name string) error
	RemoveTopic(name string) error
	AddMessages(topicName string, messages []*message.Message) error
	GetMessages(topicName string, seq uint64, maxBatch uint32) ([]*message.Message, error)
	GetLastMessage(topicName string) (*message.Message, error)
	SetConsumedSeq(topicName string, seq uint64)
	GetStoredSeq(topicName string) (uint64, error)
	Shutdown() error
}
