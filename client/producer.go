package client

import (
	"fmt"

	"github.com/dmitryikh/tube"
	"github.com/dmitryikh/tube/api"
	"github.com/dmitryikh/tube/message"
	"google.golang.org/grpc"
)

type ProducerStatus int

const (
	ProducerOnline  ProducerStatus = 0
	ProducerOffline ProducerStatus = 1
)

type Producer struct {
	config         ProducerConfig
	Status         ProducerStatus
	producedSeqMap map[string]uint64
	consumedSeqMap map[string]uint64
	storedSeqMap   map[string]uint64
	api            *api.BrokerServiceClient
	connection     *grpc.ClientConn
}

type ProducerConfig struct {
	URI      string
	DataDir  string
	MaxBatch int
}

func NewProducer(config ProducerConfig, topics []string) (*Producer, error) {
	p := &Producer{
		config:         config,
		Status:         ProducerOffline,
		producedSeqMap: make(map[string]uint64),
		consumedSeqMap: make(map[string]uint64),
		storedSeqMap:   make(map[string]uint64),
	}
	for _, topic := range topics {
		p.producedSeqMap[topic] = tube.UnsetSeq
		p.consumedSeqMap[topic] = tube.UnsetSeq
		p.storedSeqMap[topic] = tube.UnsetSeq
	}
	return p, nil
}

func (p *Producer) Produce(topic string, messages []*message.Message) error {

}

func (p *Producer) Connect() error {
	err := p.Disconnect()
	if err != nil {
		return err
	}
	connection, err := grpc.Dial(p.config.URI, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed connet to \"%s\": %s", p.config.URI, err)
	}
	p.connection = connection
	return nil
}

func (p *Producer) Disconnect() error {
	if p.connection == nil {
		return nil
	}
	connection := p.connection
	p.connection = nil
	return connection.Close()
}

func (p *Producer) ProducedSeq(topic string) uint64 {
	if seq, isFound := p.producedSeqMap[topic]; isFound {
		return seq
	}
	return tube.UnsetSeq
}

func (p *Producer) ConsumedSeq(topic string) uint64 {
	if seq, isFound := p.consumedSeqMap[topic]; isFound {
		return seq
	}
	return tube.UnsetSeq
}

func (p *Producer) StoredSeq(topic string) uint64 {
	if seq, isFound := p.storedSeqMap[topic]; isFound {
		return seq
	}
	return tube.UnsetSeq
}
