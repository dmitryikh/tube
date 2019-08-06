package broker

import (
	"context"
	"fmt"

	"github.com/dmitryikh/tube/api"
	"github.com/dmitryikh/tube/message"
)

type BrokerService struct {
	topicsManager *TopicsManager
}

func NewBrokerService(topicsManager *TopicsManager) *BrokerService {
	return &BrokerService{
		topicsManager: topicsManager,
	}
}

func (s *BrokerService) ProduceBatch(ctx context.Context, request *api.MessagesBatchRequest) (*api.MessagesBatchResponse, error) {
	// TODO: should this method be Strong Exception Guarantee?
	response := &api.MessagesBatchResponse{}
	response.ConsumedSeqs = make(map[string]uint64)
	response.ProducedSeqs = make(map[string]uint64)
	for _, messageWithRoute := range request.Messages {
		protoMessage := messageWithRoute.Message
		message := messageFromProtoMessage(protoMessage)

		err := s.topicsManager.AddMessage(messageWithRoute.Topic, message)
		if err != nil {
			// TODO: introduce error codes
			response.Error = &api.Error{
				Code:    1,
				Message: fmt.Sprintf("%s", err),
			}
			return response, nil
		}

		// TODO: make strong exception guarantee
		response.ProducedSeqs[messageWithRoute.Topic] = message.Seq
	}

	for topicName := range response.ProducedSeqs {
		seqNum, err := s.topicsManager.GetConsumedSeq(topicName)
		if err != nil {
			return nil, err
		}
		response.ConsumedSeqs[topicName] = seqNum
	}
	return response, nil
}

func (s *BrokerService) GetLastMessage(ctx context.Context, request *api.GetLastMessageRequest) (*api.GetLastMessageResponse, error) {
	response := &api.GetLastMessageResponse{}
	message, err := s.topicsManager.GetLastMessage(request.Topic)
	if err != nil {
		// TODO: code
		response.Error = &api.Error{
			Code:    1,
			Message: fmt.Sprintf("%s", err),
		}
		return response, nil
	}
	response.Message = protoMessageFromMessage(message)
	return response, nil
}

func (s *BrokerService) GetNextMessage(ctx context.Context, request *api.GetNextMessageRequest) (*api.GetNextMessageResponse, error) {
	response := &api.GetNextMessageResponse{}
	message, err := s.topicsManager.GetNextMessage(request.Topic, request.Seq)
	if err != nil {
		// TODO: code
		response.Error = &api.Error{
			Code:    1,
			Message: fmt.Sprintf("%s", err),
		}
		return response, nil
	}
	response.Message = protoMessageFromMessage(message)
	return response, nil
}

func (s *BrokerService) CreateTopic(ctx context.Context, request *api.CreateTopicsRequest) (*api.CreateTopicsResponse, error) {
	response := &api.CreateTopicsResponse{}
	err := s.topicsManager.AddTopic(request.Topic)
	if err != nil {
		// TODO: code
		response.Error = &api.Error{
			Code:    1,
			Message: fmt.Sprintf("%s", err),
		}
		return response, nil
	}
	return response, nil
}

func messageFromProtoMessage(protoMessage *api.Message) *message.Message {
	return &message.Message{
		Crc:       0,
		Seq:       protoMessage.Seq,
		Timestamp: protoMessage.Timestamp,
		Payload:   protoMessage.Payload,
		Meta:      protoMessage.Meta,
	}
}

func protoMessageFromMessage(message *message.Message) *api.Message {
	return &api.Message{
		Seq:       message.Seq,
		Timestamp: message.Timestamp,
		Payload:   message.Payload,
		Meta:      message.Meta,
	}
}
