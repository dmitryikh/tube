package client

import (
	"context"
	"fmt"

	"github.com/dmitryikh/tube/api"
	"github.com/dmitryikh/tube/message"
	"google.golang.org/grpc"
)

type Consumer struct {
	config     ConsumerConfig
	topics     []string
	api        api.BrokerServiceClient
	connection *grpc.ClientConn
}

type ConsumerConfig struct {
	URI      string
	ID       string
	MaxBatch int
}

func NewConsumer(config ConsumerConfig, topics []string) *Consumer {
	c := &Consumer{
		config: config,
		topics: topics,
	}
	return c
}

func (c *Consumer) CreateConsumer(attachStrategy string) error {
	ctx := context.Background()
	response, err := c.api.CreateConsumer(ctx, &api.CreateConsumerRequest{
		ConsumerID:     c.config.ID,
		Topics:         c.topics,
		AttachStrategy: attachStrategy,
	})
	if err != nil {
		return err
	}
	return checkResponseError(response)
}

func (c *Consumer) RecieveMessages(topic string, seq uint64) ([]*message.Message, error) {
	ctx := context.Background()

	request := &api.RecieveMessagesRequest{
		Topic:    topic,
		Seq:      seq,
		MaxBatch: uint32(c.config.MaxBatch),
	}

	response, err := c.api.RecieveMessages(ctx, request)
	if err != nil {
		return nil, err
	}
	err = checkResponseError(response)
	if err != nil {
		return nil, fmt.Errorf("RecieveMessages failed: %s", err)
	}

	messages := make([]*message.Message, 0, len(response.Messages))
	for _, protoMessage := range response.Messages {
		messages = append(messages, api.MessageFromProtoMessage(protoMessage))
	}
	return messages, nil
}

func (c *Consumer) RecieveMeta() (*api.Meta, error) {
	ctx := context.Background()
	request := &api.RecieveMetaRequest{
		Topics: c.topics,
	}

	response, err := c.api.RecieveMeta(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := checkResponseError(response); err != nil {
		return nil, fmt.Errorf("GetMeta failed: %s", err)
	}
	return api.MetaFromRecieveMetaResponse(response), nil
}

func (c *Consumer) SendMeta(consumedSeqs map[string]uint64) error {
	ctx := context.Background()
	request := &api.SendConsumerMetaRequest{
		ConsumedSeqs: consumedSeqs,
	}

	response, err := c.api.SendConsumerMeta(ctx, request)
	if err != nil {
		return err
	}
	if err := checkResponseError(response); err != nil {
		return fmt.Errorf("SendMeta failed: %s", err)
	}
	return nil
}

func (c *Consumer) Connect() error {
	err := c.Disconnect()
	if err != nil {
		return err
	}
	connection, err := grpc.Dial(c.config.URI, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed connet to \"%s\": %s", c.config.URI, err)
	}
	c.connection = connection
	c.api = api.NewBrokerServiceClient(c.connection)

	return nil
}

func (c *Consumer) Disconnect() error {
	if c.connection == nil {
		return nil
	}
	connection := c.connection
	c.connection = nil
	c.api = nil
	return connection.Close()
}
