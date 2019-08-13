package broker

import (
	"context"
	"fmt"

	"github.com/dmitryikh/tube/api"
)

type BrokerService struct {
	topicsManager *TopicsManager
}

func NewBrokerService(topicsManager *TopicsManager) *BrokerService {
	return &BrokerService{
		topicsManager: topicsManager,
	}
}

func (s *BrokerService) SendMessages(ctx context.Context, request *api.SendMessagesRequest) (*api.SendMessagesResponse, error) {
	// TODO: should this method be Strong Exception Guarantee?
	response := &api.SendMessagesResponse{}
	// response.ConsumedSeqs = make(map[string]uint64)
	// response.ProducedSeqs = make(map[string]uint64)
	for _, messageWithRoute := range request.Messages {
		protoMessage := messageWithRoute.Message
		message := api.MessageFromProtoMessage(protoMessage)

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
		// response.ProducedSeqs[messageWithRoute.Topic] = message.Seq
	}

	// for topicName := range response.ProducedSeqs {
	// 	seqNum, err := s.topicsManager.GetConsumedSeq(topicName)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	response.ConsumedSeqs[topicName] = seqNum
	// }
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
	response.Message = api.ProtoMessageFromMessage(message)
	return response, nil
}

func (s *BrokerService) RecieveMessages(ctx context.Context, request *api.RecieveMessagesRequest) (*api.RecieveMessagesResponse, error) {
	response := &api.RecieveMessagesResponse{}
	messages, err := s.topicsManager.GetMessages(request.Topic, request.Seq, request.MaxBatch)
	if err != nil {
		// TODO: code
		response.Error = &api.Error{
			Code:    1,
			Message: fmt.Sprintf("%s", err),
		}
		return response, nil
	}
	for _, message := range messages {
		response.Messages = append(response.Messages, api.ProtoMessageFromMessage(message))
	}
	return response, nil
}

func (s *BrokerService) CreateTopic(ctx context.Context, request *api.CreateTopicsRequest) (*api.CreateTopicsResponse, error) {
	response := &api.CreateTopicsResponse{}
	err := s.topicsManager.AddTopic(request.Topic)
	if err != nil {
		// TODO: code
		response.Error = api.ProtoErrorFromError(err)
		return response, nil
	}
	return response, nil
}

func (s *BrokerService) RecieveMeta(ctx context.Context, request *api.RecieveMetaRequest) (*api.RecieveMetaResponse, error) {
	response := &api.RecieveMetaResponse{}
	meta := api.NewMeta()
	for _, topicName := range request.Topics {
		consumedSeq, err := s.topicsManager.GetConsumedSeq(topicName)
		if err != nil {
			response.Error = &api.Error{
				Code:    1,
				Message: fmt.Sprintf("%s", err),
			}
			return response, nil
		}
		meta.ConsumedSeqs[topicName] = consumedSeq
	}

	for _, topicName := range request.Topics {
		storedSeq, err := s.topicsManager.GetStoredSeq(topicName)
		if err != nil {
			response.Error = &api.Error{
				Code:    1,
				Message: fmt.Sprintf("%s", err),
			}
			return response, nil
		}
		meta.StoredSeqs[topicName] = storedSeq
	}

	return api.RecieveMetaResponseFromMeta(meta), nil
}

func (s *BrokerService) SendMeta(ctx context.Context, request *api.SendMetaRequest) (*api.SendMetaResponse, error) {
	// only consumed seqs are useful for now
	for topicName, seq := range request.ConsumedSeqs {
		s.topicsManager.SetConsumedSeq(topicName, seq)
	}
	return &api.SendMetaResponse{}, nil
}
