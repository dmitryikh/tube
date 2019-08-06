package storage

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/dmitryikh/tube"
	"github.com/dmitryikh/tube/message"
)

type SegmentHeader struct {
	MessagesCount uint64
	SeqMin        uint64
	SeqMax        uint64
	TimestampMin  uint64
	TimestampMax  uint64
}

func NewSegmentHeader() *SegmentHeader {
	return &SegmentHeader{
		MessagesCount: 0,
		SeqMin:        tube.UnsetSeq,
		SeqMax:        tube.UnsetSeq,
		TimestampMin:  0,
		TimestampMax:  0,
	}
}

func (h *SegmentHeader) SkippedMessagesCount() uint64 {
	return (h.SeqMax - h.SeqMin) - h.MessagesCount
}

func (h *SegmentHeader) Serialize(writer io.Writer) error {
	// first 4 bytes - reserved for version
	version := uint32(0)
	err := tube.WriteUint32(writer, version)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(writer)
	err = encoder.Encode(h.MessagesCount)
	if err != nil {
		return err
	}
	err = encoder.Encode(h.SeqMin)
	if err != nil {
		return err
	}
	err = encoder.Encode(h.SeqMax)
	if err != nil {
		return err
	}
	err = encoder.Encode(h.TimestampMin)
	if err != nil {
		return err
	}
	err = encoder.Encode(h.TimestampMax)
	if err != nil {
		return err
	}
	return nil
}

func (h *SegmentHeader) Deserialize(reader io.Reader) error {
	// first 4 bytes - reserved for version
	version, err := tube.ReadUint32(reader)
	if err != nil {
		return err
	}
	if version == 0 {
		decoder := gob.NewDecoder(reader)
		err = decoder.Decode(&h.MessagesCount)
		if err != nil {
			return err
		}
		err = decoder.Decode(&h.SeqMin)
		if err != nil {
			return err
		}
		err = decoder.Decode(&h.SeqMax)
		if err != nil {
			return err
		}
		err = decoder.Decode(&h.TimestampMin)
		if err != nil {
			return err
		}
		err = decoder.Decode(&h.TimestampMax)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unsupported version %d", version)
	}
	return nil
}

type ActiveSegment struct {
	Header          *SegmentHeader
	Messages        []*message.Message
	SegmentFilePath string
}

func NewActiveSegment(segmentFilePath string) *ActiveSegment {
	activeSegment := &ActiveSegment{
		Header:          NewSegmentHeader(),
		Messages:        make([]*message.Message, 0),
		SegmentFilePath: segmentFilePath,
	}
	return activeSegment
}

func (s *ActiveSegment) AddMessage(message *message.Message) error {
	if s.Header.SeqMax == tube.UnsetSeq {
		// first message in segment
		s.Header.SeqMin = message.Seq
	} else if message.Seq <= s.Header.SeqMax {
		return fmt.Errorf("message with seq = %d already exists", message.Seq)
	}
	s.Messages = append(s.Messages, message)
	s.Header.MessagesCount++
	s.Header.SeqMax = message.Seq
	s.Header.TimestampMin = tube.MinUint64(s.Header.TimestampMin, message.Timestamp)
	s.Header.TimestampMax = tube.MaxUint64(s.Header.TimestampMax, message.Timestamp)
	return nil
}

func (s *ActiveSegment) GetNextMessage(seq uint64) (*message.Message, error) {
	if seq < s.Header.SeqMin || seq > s.Header.SeqMax {
		return nil, fmt.Errorf("no message with seq = %d in segment [%d; %d]", seq, s.Header.SeqMin, s.Header.SeqMax)
	}
	idx := sort.Search(len(s.Messages), func(i int) bool {
		return s.Messages[i].Seq > seq
	})

	if idx == len(s.Messages) {
		return nil, fmt.Errorf("message with seq = %d seems to be skipped in segment [%d; %d]", seq, s.Header.SeqMin, s.Header.SeqMax)
	}

	return s.Messages[idx], nil
}

func (s *ActiveSegment) Serialize(writer io.Writer) error {
	err := s.Header.Serialize(writer)
	if err != nil {
		return err
	}
	for _, m := range s.Messages {
		err := m.Serialize(writer)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ActiveSegment) Deserialize(reader io.Reader) error {
	err := s.Header.Deserialize(reader)
	if err != nil {
		return err
	}
	s.Messages = make([]*message.Message, 0, s.Header.MessagesCount)

	for i := uint64(0); i < s.Header.MessagesCount; i++ {
		message := message.NewMessage()
		err = message.Deserialize(reader, true)
		if err != nil {
			return err
		}
		s.Messages = append(s.Messages, message)
	}
	return nil
}

func (s *ActiveSegment) SaveToFile(fileNamePath string) error {
	tmpFileNamePath := fileNamePath + ".tmp"
	file, err := os.Create(tmpFileNamePath)
	if err != nil {
		return err
	}

	err = s.Serialize(file)
	if err != nil {
		file.Close()
		os.Remove(tmpFileNamePath)
		return err
	}

	err = file.Close()
	if err != nil {
		os.Remove(tmpFileNamePath)
		return err
	}

	err = os.Rename(tmpFileNamePath, fileNamePath)
	if err != nil {
		os.Remove(tmpFileNamePath)
		return err
	}

	if s.SegmentFilePath != "" && s.SegmentFilePath != fileNamePath &&
		tube.IsRegularFile(s.SegmentFilePath) {
		err = os.Remove(s.SegmentFilePath)
		if err != nil {
			return err
		}
	}
	s.SegmentFilePath = fileNamePath

	return nil
}

type Segment struct {
	Header *SegmentHeader
}

func NewSegment() *Segment {
	return &Segment{
		Header: NewSegmentHeader(),
	}
}

func (s *Segment) Deserialize(reader io.Reader) error {
	err := s.Header.Deserialize(reader)
	if err != nil {
		return err
	}
	// TODO: do we need to read messages here?
	// for i := uint64(0); i < s.Header.MessagesCount; i++ {
	// 	message := message.NewMessage()
	// 	err = message.Deserialize(reader, false)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	return nil
}
