package broker

type Topic struct {
	ConsumedSeq uint64
	ProducedSeq uint64
}

func NewTopic() Topic {
	return Topic{
		ConsumedSeq: 0,
		ProducedSeq: 0,
	}
}
