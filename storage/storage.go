package storage

import (
	"github.com/dmitryikh/tube/message"
)

type Storage interface {
	AddTopic(name string) error
	RemoveTopic(name string) error
	AddMessage(topicName string, msg *message.Message) error
	GetNextMessage(topicName string, seq uint64) (*message.Message, error)
	GetLastMessage(topicName string) (*message.Message, error)
	Shutdown() error
}
