package storage

import (
	"fmt"
	"math"

	"github.com/dmitryikh/tube/message"
)

type StopIterationError struct{}

func (e StopIterationError) Error() string {
	return "end of iteration"
}

const (
	voidIdx = math.MaxInt32
)

// Should be true:
// number of segments doesn't changed
// only active segment can be changed (messages added)

type SegmentedIterator struct {
	segmentIdx int
	messageIdx int
	segments   []*Segment
}

func NewSegmentedIterator(segments []*Segment) *SegmentedIterator {
	return &SegmentedIterator{
		segmentIdx: voidIdx,
		messageIdx: voidIdx,
		segments:   segments,
	}
}

func (i *SegmentedIterator) Reset() {
	i.segmentIdx = voidIdx
	i.messageIdx = voidIdx
}

func (i *SegmentedIterator) Seek(seq uint64) error {
	foundSegmentIdx := -1
	for k := 0; k < len(i.segments); k++ {
		if i.segments[k].Header.SeqMax > seq {
			foundSegmentIdx = k
			break
		}
	}

	if foundSegmentIdx == -1 {
		i.Reset()
		// TODO: do we need error here?
		// seqMax := i.segments[len(i.segments)-1].Header.SeqMax
		// return fmt.Errorf("seq >= seqMax (%d >= %d)", seq, seqMax)
		return nil
	}

	foundSegment := i.segments[foundSegmentIdx]
	if foundSegment.IsPartiallyLoaded() {
		err := foundSegment.LoadMessages()
		if err != nil {
			i.Reset()
			return fmt.Errorf("error while loading segment %s: %s", foundSegment.SegmentFilePath, err)
		}
	}
	idx, err := foundSegment.GetIndexOfNextSeqNum(seq)
	if err != nil {
		i.Reset()
		return nil
	}

	i.segmentIdx = foundSegmentIdx
	i.messageIdx = idx
	return nil
}

func (i *SegmentedIterator) SeekEnd() error {
	nSegments := len(i.segments)
	activeSegment := i.segments[nSegments-1]
	if activeSegment.IsEmpty() {
		if nSegments > 1 {
			prevSegment := i.segments[nSegments-2]
			if prevSegment.IsPartiallyLoaded() {
				err := prevSegment.LoadMessages()
				if err != nil {
					i.Reset()
					return fmt.Errorf("error while loading segment %s: %s", prevSegment.SegmentFilePath, err)
				}
			}
			i.messageIdx = len(prevSegment.Messages) - 1
			i.segmentIdx = nSegments - 2
		} else {
			i.Reset()
		}
		return nil
	}
	i.segmentIdx = nSegments - 1
	i.messageIdx = len(activeSegment.Messages) - 1
	return nil
}

func (i *SegmentedIterator) Next() (*message.Message, error) {
	if i.segmentIdx == voidIdx || i.messageIdx == voidIdx {
		return nil, StopIterationError{}
	}

	if i.isActiveSegment(i.segmentIdx) {
		activeSegment := i.segments[i.segmentIdx]
		if len(activeSegment.Messages) <= i.messageIdx {
			return nil, StopIterationError{}
		}
		msg, err := activeSegment.GetMessageByIdx(i.messageIdx)
		if err != nil {
			return nil, err
		}
		i.messageIdx += 1
		return msg, nil
	} else {
		segment := i.segments[i.segmentIdx]
		// TODO: can be the case: when messages unloaded right after the PartiallyLoaded check
		messages := segment.Messages
		if segment.IsPartiallyLoaded() {
			err := segment.LoadMessages()
			if err != nil {
				return nil, fmt.Errorf("error while loading segment %s: %s", segment.SegmentFilePath, err)
			}
			messages = segment.Messages
		}
		msg, err := segment.GetMessageByIdx(i.messageIdx)
		if err != nil {
			return nil, err
		}
		i.messageIdx += 1
		if i.messageIdx >= len(messages) {
			i.messageIdx = 0
			i.segmentIdx += 1
		}
		return msg, nil
	}
}

func (i *SegmentedIterator) isActiveSegment(segmentIdx int) bool {
	return segmentIdx == (len(i.segments) - 1)
}
