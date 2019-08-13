package api

import "github.com/dmitryikh/tube/message"

func MessageFromProtoMessage(protoMessage *Message) *message.Message {
	return &message.Message{
		Crc:       0,
		Seq:       protoMessage.Seq,
		Timestamp: protoMessage.Timestamp,
		Payload:   protoMessage.Payload,
		Meta:      protoMessage.Meta,
	}
}

func ProtoMessageFromMessage(message *message.Message) *Message {
	return &Message{
		Seq:       message.Seq,
		Timestamp: message.Timestamp,
		Payload:   message.Payload,
		Meta:      message.Meta,
	}
}

type Meta struct {
	ProducedSeqs map[ /*topicName*/ string] /*seqNum*/ uint64
	ConsumedSeqs map[ /*topicName*/ string] /*seqNum*/ uint64
	StoredSeqs   map[ /*topicName*/ string] /*seqNum*/ uint64
}

func NewMeta() *Meta {
	return &Meta{
		ProducedSeqs: make(map[string]uint64),
		ConsumedSeqs: make(map[string]uint64),
		StoredSeqs:   make(map[string]uint64),
	}
}

func MetaFromRecieveMetaResponse(response *RecieveMetaResponse) *Meta {
	meta := NewMeta()
	for topicName, seq := range response.ConsumedSeqs {
		meta.ConsumedSeqs[topicName] = seq
	}
	for topicName, seq := range response.ProducedSeqs {
		meta.ProducedSeqs[topicName] = seq
	}
	for topicName, seq := range response.StoredSeqs {
		meta.StoredSeqs[topicName] = seq
	}
	return meta
}

func RecieveMetaResponseFromMeta(meta *Meta) *RecieveMetaResponse {
	response := &RecieveMetaResponse{
		ProducedSeqs: make(map[string]uint64),
		ConsumedSeqs: make(map[string]uint64),
		StoredSeqs:   make(map[string]uint64),
	}
	for topicName, seq := range meta.ConsumedSeqs {
		response.ConsumedSeqs[topicName] = seq
	}
	for topicName, seq := range meta.ProducedSeqs {
		response.ProducedSeqs[topicName] = seq
	}
	for topicName, seq := range meta.StoredSeqs {
		response.StoredSeqs[topicName] = seq
	}
	return response
}

// func SendMetaRequestFromMeta(meta *Meta) *SendMetaRequest {
// 	request := &SendMetaRequest{
// 		ProducedSeqs: make(map[string]uint64),
// 		ConsumedSeqs: make(map[string]uint64),
// 		StoredSeqs:   make(map[string]uint64),
// 	}
// 	for topicName, seq := range meta.ConsumedSeqs {
// 		request.ConsumedSeqs[topicName] = seq
// 	}
// 	for topicName, seq := range meta.ProducedSeqs {
// 		request.ProducedSeqs[topicName] = seq
// 	}
// 	for topicName, seq := range meta.StoredSeqs {
// 		request.StoredSeqs[topicName] = seq
// 	}
// 	return request
// }
