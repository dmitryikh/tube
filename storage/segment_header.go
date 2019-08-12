package storage

import (
	"encoding/gob"
	"fmt"
	"io"

	"github.com/dmitryikh/tube"
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
		SeqMin:        0,
		SeqMax:        0,
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
