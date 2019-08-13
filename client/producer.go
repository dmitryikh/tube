package client

import (
	"context"
	"fmt"
	"reflect"

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
	api            api.BrokerServiceClient
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
		p.producedSeqMap[topic] = 0
		p.consumedSeqMap[topic] = 0
		p.storedSeqMap[topic] = 0
	}

	err := p.Connect()
	if err != nil {
		return nil, err
	}
	err = p.GetMeta()
	if err != nil {
		return nil, err
	}
	// set produced seq as consumed seq
	for topicName, seq := range p.consumedSeqMap {
		p.producedSeqMap[topicName] = seq
	}
	return p, nil
}

func (p *Producer) SendMessages(topic string, messages []*message.Message) error {
	ctx := context.Background()
	apiMessages := make([]*api.MessageWithRoute, 0, len(messages))

	producedSeq, isFound := p.producedSeqMap[topic]
	if !isFound {
		return fmt.Errorf("producer isn't subscribed to topic \"%s\"", topic)
	}
	request := &api.SendMessagesRequest{}
	for _, msg := range messages {
		if msg.Seq <= producedSeq {
			return fmt.Errorf("message with seq = %d (topic = \"%s\") already produced", msg.Seq, topic)
		}
		producedSeq = msg.Seq
		apiMsg := api.ProtoMessageFromMessage(msg)
		apiMessages = append(apiMessages, &api.MessageWithRoute{
			Message: apiMsg,
			Topic:   topic,
		})
	}
	request.Messages = apiMessages

	response, err := p.api.SendMessages(ctx, request)
	if err != nil {
		return err
	}
	if err := checkResponseError(response); err != nil {
		return fmt.Errorf("SendMessages failed: %s", err)
	}
	p.producedSeqMap[topic] = producedSeq
	return nil
}

func (p *Producer) GetLastMessage(topic string) (*message.Message, error) {
	ctx := context.Background()
	request := &api.GetLastMessageRequest{
		Topic: topic,
	}
	response, err := p.api.GetLastMessage(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := checkResponseError(response); err != nil {
		return nil, fmt.Errorf("GetLastMessage failed: %s", err)
	}
	return api.MessageFromProtoMessage(response.Message), nil
}

func (p *Producer) GetMeta() error {
	ctx := context.Background()
	topics := make([]string, 0, len(p.producedSeqMap))
	for topicName, _ := range p.producedSeqMap {
		topics = append(topics, topicName)
	}
	request := &api.RecieveMetaRequest{
		Topics: topics,
	}

	response, err := p.api.RecieveMeta(ctx, request)
	if err != nil {
		return err
	}
	if err := checkResponseError(response); err != nil {
		return fmt.Errorf("GetMeta failed: %s", err)
	}
	p.consumedSeqMap = response.ConsumedSeqs
	p.storedSeqMap = response.StoredSeqs
	return nil
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
	return 0
}

func (p *Producer) ConsumedSeq(topic string) uint64 {
	if seq, isFound := p.consumedSeqMap[topic]; isFound {
		return seq
	}
	return 0
}

func (p *Producer) StoredSeq(topic string) uint64 {
	if seq, isFound := p.storedSeqMap[topic]; isFound {
		return seq
	}
	return 0
}

func checkResponseError(response interface{}) error {
	v := reflect.Indirect(reflect.ValueOf(response))
	erro, isOk := v.FieldByName("Error").Interface().(*api.Error)
	if !isOk {
		return fmt.Errorf("bad error cast")
	}
	if erro != nil {
		return fmt.Errorf("%s (code %d)", erro.Message, erro.Code)
	}
	return nil
}
