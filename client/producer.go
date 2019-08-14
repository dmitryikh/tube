package client

import (
	"context"
	"fmt"

	"github.com/dmitryikh/tube/api"
	"github.com/dmitryikh/tube/message"
	"google.golang.org/grpc"
)

type Producer struct {
	config     ProducerConfig
	topics     []string
	api        api.BrokerServiceClient
	connection *grpc.ClientConn
}

type ProducerConfig struct {
	URI string
}

func NewProducer(config ProducerConfig, topics []string) *Producer {
	return &Producer{
		config: config,
		topics: topics,
	}
}

func (p *Producer) CreateTopic(topic string) error {
	ctx := context.Background()
	request := &api.CreateTopicsRequest{
		Topic: topic,
	}
	response, err := p.api.CreateTopic(ctx, request)
	if err != nil {
		return err
	}
	if response.Error != nil {
		return api.ErrorFromProtoError(response.Error)
	}
	return nil
}

func (p *Producer) SendMessages(topic string, messages []*message.Message) error {
	ctx := context.Background()
	apiMessages := make([]*api.MessageWithRoute, 0, len(messages))

	request := &api.SendMessagesRequest{}
	for _, msg := range messages {
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
		return nil, api.ErrorFromProtoError(response.Error)
	}
	return api.MessageFromProtoMessage(response.Message), nil
}

func (p *Producer) RecieveMeta() (*api.Meta, error) {
	ctx := context.Background()
	request := &api.RecieveMetaRequest{
		Topics: p.topics,
	}

	response, err := p.api.RecieveMeta(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := checkResponseError(response); err != nil {
		return nil, fmt.Errorf("SendMeta failed: %s", err)
	}
	return api.MetaFromRecieveMetaResponse(response), nil
}

func (p *Producer) SendMeta(producedSeqs map[string]uint64) error {
	ctx := context.Background()
	request := &api.SendProducerMetaRequest{
		ProducedSeqs: producedSeqs,
	}

	response, err := p.api.SendProducerMeta(ctx, request)
	if err != nil {
		return err
	}
	if err := checkResponseError(response); err != nil {
		return fmt.Errorf("GetMeta failed: %s", err)
	}
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
	p.api = api.NewBrokerServiceClient(p.connection)

	return nil
}

func (p *Producer) Disconnect() error {
	if p.connection == nil {
		return nil
	}
	connection := p.connection
	p.connection = nil
	p.api = nil
	return connection.Close()
}
