package broker

import (
	"github.com/dmitryikh/tube"
)

type Topic struct {
	ConsumedSeq uint64
}

func NewTopic() Topic {
	return Topic{
		ConsumedSeq: tube.UnsetSeq,
	}
}
