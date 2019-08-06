package tube

import (
	"math"
)

const (
	UnsetSeq = math.MaxUint64
)

// Key - topic name
// Value - sequantial number associated
type SeqSet map[string]uint64

func NewSeqSet() SeqSet {
	return make(map[string]uint64)
}

func (s *SeqSet) UpdateMin(seqs SeqSet) {
	for topicName, seq := range seqs {
		if seqOrig, isFound := (*s)[topicName]; isFound {
			if seqOrig == UnsetSeq {
				seqOrig = seq
			}
			(*s)[topicName] = MinUint64(seq, seqOrig)
		}
	}
}

func (s *SeqSet) UpdateMax(seqs SeqSet) {
	for topicName, seq := range seqs {
		if seqOrig, isFound := (*s)[topicName]; isFound {
			if seqOrig == UnsetSeq {
				seqOrig = seq
			}
			(*s)[topicName] = MaxUint64(seq, seqOrig)
		}
	}
}
