package broker

import (
	"context"
	"fmt"

	"github.com/dmitryikh/tube"
	"github.com/dmitryikh/tube/api"
)

type BrokerService struct {
	broker *Broker
}

func NewBrokerService(broker *Broker) *BrokerService {
	return &BrokerService{
		broker: broker,
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

		err := s.broker.TopicsManager.AddMessage(messageWithRoute.Topic, message)
		if err != nil {
			response.Error = api.ProtoErrorFromError(err)
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
	message, err := s.broker.TopicsManager.GetLastMessage(request.Topic)
	if err != nil {
		response.Error = api.ProtoErrorFromError(err)
		return response, nil
	}
	response.Message = api.ProtoMessageFromMessage(message)
	return response, nil
}

func (s *BrokerService) RecieveMessages(ctx context.Context, request *api.RecieveMessagesRequest) (*api.RecieveMessagesResponse, error) {
	response := &api.RecieveMessagesResponse{}
	messages, err := s.broker.TopicsManager.GetMessages(request.Topic, request.Seq, request.MaxBatch)
	if err != nil {
		response.Error = api.ProtoErrorFromError(err)
		return response, nil
	}
	for _, message := range messages {
		response.Messages = append(response.Messages, api.ProtoMessageFromMessage(message))
	}
	return response, nil
}

func (s *BrokerService) CreateTopic(ctx context.Context, request *api.CreateTopicsRequest) (*api.CreateTopicsResponse, error) {
	response := &api.CreateTopicsResponse{}
	err := s.broker.TopicsManager.AddTopic(request.Topic)
	if err != nil {
		response.Error = api.ProtoErrorFromError(err)
		return response, nil
	}
	return response, nil
}

func (s *BrokerService) CreateConsumer(ctx context.Context, request *api.CreateConsumerRequest) (*api.CreateConsumerResponse, error) {
	response := &api.CreateConsumerResponse{}
	seqSet := tube.NewSeqSet()
	if request.AttachStrategy == "latest" {
		for _, topicName := range request.Topics {
			msg, err := s.broker.TopicsManager.GetLastMessage(topicName)
			if err != nil {
				seqSet[topicName] = tube.UnsetSeq
			} else {
				seqSet[topicName] = msg.Seq
			}
		}
	} else if request.AttachStrategy == "earliest" {
		consumedSeqs := s.broker.ConsumersRegistry.GetAllTopicsSeqs()
		for _, topicName := range request.Topics {
			if seq, isFound := consumedSeqs[topicName]; isFound {
				seqSet[topicName] = seq
			} else {
				seqSet[topicName] = tube.UnsetSeq
			}
		}
	} else {
		response.Error = api.ProtoErrorFromError(
			fmt.Errorf("unknown attach strategy \"%s\"", request.AttachStrategy),
		)
		return response, nil
	}
	err := s.broker.ConsumersRegistry.AddConsumer(request.ConsumerID, seqSet)
	if err != nil {
		response.Error = api.ProtoErrorFromError(err)
		return response, nil
	}
	return response, nil
}

func (s *BrokerService) RecieveMeta(ctx context.Context, request *api.RecieveMetaRequest) (*api.RecieveMetaResponse, error) {
	response := &api.RecieveMetaResponse{}
	meta := api.NewMeta()

	// fill consumedSeqs from ConsumerRegistry
	consumedSeqs := s.broker.ConsumersRegistry.GetAllTopicsSeqs()
	for _, topicName := range request.Topics {
		if seq, isFound := consumedSeqs[topicName]; isFound {
			meta.ConsumedSeqs[topicName] = seq
		} else {
			meta.ConsumedSeqs[topicName] = tube.UnsetSeq
		}
	}

	for _, topicName := range request.Topics {
		storedSeq, err := s.broker.TopicsManager.GetStoredSeq(topicName)
		if err != nil {
			response.Error = api.ProtoErrorFromError(err)
			return response, nil
		}
		meta.StoredSeqs[topicName] = storedSeq
	}

	for _, topicName := range request.Topics {
		seq, err := s.broker.TopicsManager.GetProducedSeq(topicName)
		if err != nil {
			response.Error = api.ProtoErrorFromError(err)
			return response, nil
		}
		meta.ProducedSeqs[topicName] = seq
	}

	// availableSeq - seq from last available message in the broker (or 0 if no messages yet)
	for _, topicName := range request.Topics {
		msg, err := s.broker.TopicsManager.GetLastMessage(topicName)
		seq := tube.UnsetSeq
		if err != nil {
			if _, ok := err.(tube.TopicEmptyError); !ok {
				response.Error = api.ProtoErrorFromError(err)
				return response, nil
			}
		} else {
			seq = msg.Seq
		}
		meta.AvailableSeqs[topicName] = seq
	}

	return api.RecieveMetaResponseFromMeta(meta), nil
}

func (s *BrokerService) SendProducerMeta(ctx context.Context, request *api.SendProducerMetaRequest) (*api.SendMetaResponse, error) {
	for topicName, seq := range request.ProducedSeqs {
		s.broker.TopicsManager.SetProducedSeq(topicName, seq)
	}
	return &api.SendMetaResponse{}, nil
}

func (s *BrokerService) SendConsumerMeta(ctx context.Context, request *api.SendConsumerMetaRequest) (*api.SendMetaResponse, error) {
	response := &api.SendMetaResponse{}
	err := s.broker.ConsumersRegistry.SetConsumedSeqs(request.ConsumerID, request.ConsumedSeqs)
	if err != nil {
		response.Error = api.ProtoErrorFromError(err)
		return response, nil
	}
	return response, nil
}
