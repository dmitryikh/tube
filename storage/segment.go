package storage

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/dmitryikh/tube"
	"github.com/dmitryikh/tube/message"
)

type Segment struct {
	Header          *SegmentHeader
	Messages        []*message.Message
	SegmentFilePath string
	LastMessageRead time.Time
	PartiallyLoaded bool
	lock            sync.RWMutex
	messagesSize    int
}

func NewSegment() *Segment {
	activeSegment := &Segment{
		Header:          NewSegmentHeader(),
		Messages:        make([]*message.Message, 0),
		SegmentFilePath: "",
		LastMessageRead: time.Now(),
		PartiallyLoaded: false,
		messagesSize:    0,
	}
	return activeSegment
}

func SegmentFromFile(segmentFilepath string, loadMessages bool) (*Segment, error) {
	file, err := os.Open(segmentFilepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	bufReader := bufio.NewReader(file)
	segment := NewSegment()
	err = segment.Deserialize(bufReader, loadMessages)
	if err != nil {
		return nil, err
	}
	segment.SegmentFilePath = segmentFilepath
	// TODO: check consistency between segment filename and loaded data
	return segment, nil
}

func (s *Segment) LoadMessages() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	file, err := os.Open(s.SegmentFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	bufReader := bufio.NewReader(file)
	err = s.Deserialize(bufReader, true /*loadMessages*/)
	if err != nil {
		return err
	}
	s.LastMessageRead = time.Now()
	// TODO: check consistency between segment filename and loaded data
	return nil
}

func (s *Segment) UnloadMessages() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	file, err := os.Open(s.SegmentFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	bufReader := bufio.NewReader(file)
	err = s.Deserialize(bufReader, false /*loadMessages*/)
	if err != nil {
		return err
	}
	// TODO: check consisbency between segment filename and loaded data
	return nil
}

func (s *Segment) AddMessages(messages []*message.Message) error {
	if len(messages) == 0 {
		return nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.PartiallyLoaded {
		return fmt.Errorf("Can't add messages to partially loaded segment")
	}
	if messages[0].Seq <= s.Header.SeqMax {
		return fmt.Errorf("seq less that current SeqMax (%d < %d)", messages[0].Seq, s.Header.SeqMax)
	}
	s.Messages = append(s.Messages, messages...)
	s.Header.MessagesCount += uint64(len(messages))
	s.Header.SeqMax = messages[len(messages)-1].Seq
	if s.Header.SeqMin == 0 {
		// first message
		s.Header.SeqMin = messages[0].Seq
	}

	for _, message := range messages {
		s.Header.TimestampMin = tube.MinUint64(s.Header.TimestampMin, message.Timestamp)
		s.Header.TimestampMax = tube.MaxUint64(s.Header.TimestampMax, message.Timestamp)
		s.messagesSize += message.Size()
	}
	return nil
}

func (s *Segment) getIndexOfNextSeqNum(seq uint64) (int, error) {
	if seq >= s.Header.SeqMax {
		return 0, fmt.Errorf("no message with seq > %d in segment [%d; %d]", seq, s.Header.SeqMin, s.Header.SeqMax)
	}
	idx := sort.Search(len(s.Messages), func(i int) bool {
		return s.Messages[i].Seq > seq
	})

	if idx == len(s.Messages) {
		return 0, fmt.Errorf("logicError: no next message for seq > %d (segment [%d; %d])", seq, s.Header.SeqMin, s.Header.SeqMax)
	}

	return idx, nil
}

func (s *Segment) GetIndexOfNextSeqNum(seq uint64) (int, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.PartiallyLoaded {
		return 0, fmt.Errorf("Can't read messages from partially loaded segment")
	}
	return s.getIndexOfNextSeqNum(seq)
}

// func (s *Segment) GetNextMessage(seq uint64) (*message.Message, error) {
// 	s.lock.RLock()
// 	defer s.lock.RUnlock()
// 	if s.PartiallyLoaded {
// 		return nil, fmt.Errorf("Can't read messages from partially loaded segment")
// 	}
// 	s.LastMessageRead = time.Now()
// 	idx, err := s.getIndexOfNextSeqNum(seq)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return s.Messages[idx], nil
// }

func (s *Segment) GetMessageByIdx(idx int) (*message.Message, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.PartiallyLoaded {
		return nil, fmt.Errorf("Can't read messages from partially loaded segment")
	}
	s.LastMessageRead = time.Now()
	if idx >= len(s.Messages) {
		return nil, fmt.Errorf("Out of bounds (%d >= %d)", idx, len(s.Messages))
	}
	return s.Messages[idx], nil
}

// func (s *Segment) getMessageByIdx(idx int) (*message.Message, error) {
// 	s.lock.RLock()
// 	// defer s.lock.RUnlock()
// 	if s.PartiallyLoaded {
// 		s.lock.RUnlock()
// 		s.lock.Lock()
// 		defer s.lock.Unlock()
// 		err := s.LoadMessages()
// 		if err != nil {
// 			return nil, err
// 		}
// 		if idx >= len(s.Messages) {
// 			return nil, fmt.Errorf("Out of bounds (%d >= %d)", idx, len(s.Messages))
// 		}
// 		s.LastMessageRead = time.Now()
// 		return s.Messages[idx], nil
// 	} else {
// 		defer s.lock.Unlock()
// 		if idx >= len(s.Messages) {
// 			return nil, fmt.Errorf("Out of bounds (%d >= %d)", idx, len(s.Messages))
// 		}
// 		s.LastMessageRead = time.Now()
// 		return s.Messages[idx], nil
// 	}
// }

func (s *Segment) GetLastMessage() (*message.Message, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.PartiallyLoaded {
		return nil, fmt.Errorf("Can't read messages from partially loaded segment")
	}
	if s.IsEmpty() {
		return nil, fmt.Errorf("segment is empty")
	}
	return s.Messages[len(s.Messages)-1], nil
}

func (s *Segment) Serialize(writer io.Writer) error {
	if s.PartiallyLoaded {
		return fmt.Errorf("Can't serialize partially loaded segment")
	}
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

func (s *Segment) Deserialize(reader io.Reader, loadMessages bool) error {
	err := s.Header.Deserialize(reader)
	if err != nil {
		return err
	}
	if loadMessages {
		s.PartiallyLoaded = false
		s.Messages = make([]*message.Message, 0, s.Header.MessagesCount)

		s.messagesSize = 0
		for i := uint64(0); i < s.Header.MessagesCount; i++ {
			message := message.NewMessage()
			err = message.Deserialize(reader, true)
			if err != nil {
				return err
			}
			s.Messages = append(s.Messages, message)
			s.messagesSize += message.Size()
		}
	} else {
		s.PartiallyLoaded = true
		s.Messages = make([]*message.Message, 0)
		s.messagesSize = 0
	}
	return nil
}

func (s *Segment) SaveToFile(fileNamePath string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.PartiallyLoaded {
		return fmt.Errorf("Can't save partially loaded segment")
	}
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

func (s *Segment) AppendToFile(filenamePath string) error {
	// consider messages are immutable (no one can change already written messages)
	// 1. Append only new messages to the end of the file
	// 2. Update segment header

	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.PartiallyLoaded {
		return fmt.Errorf("Can't save partially loaded segment")
	}

	if s.SegmentFilePath == "" {
		// Create new file if no old one
		return s.SaveToFile(filenamePath)
	}

	segmentOrig, err := SegmentFromFile(s.SegmentFilePath, false)
	if err != nil {
		return err
	}
	seqMin := segmentOrig.Header.SeqMin
	seqMax := segmentOrig.Header.SeqMax

	if seqMin != s.Header.SeqMin {
		return fmt.Errorf("seqMin should be the same (from file: %d, from active segment: %d)", seqMin, s.Header.SeqMin)
	}

	if seqMax > s.Header.SeqMax {
		return fmt.Errorf("file seqMax > segment seqMax (%d > %d)", seqMax, s.Header.SeqMax)
	}

	if seqMax == s.Header.SeqMax {
		// no new messages to append
		return nil
	}

	startIdx, err := s.getIndexOfNextSeqNum(seqMax)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(s.SegmentFilePath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	lastOffsetBytes, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return err
	}
	lastWrittenSeq := seqMax
	err = nil
	for idx := startIdx; idx < len(s.Messages); idx++ {
		msg := s.Messages[idx]
		err = msg.Serialize(file)
		if err != nil {
			// delete new messages to restore the file as it was before appending
			_ = file.Truncate(lastOffsetBytes)
			_ = file.Close()
			return err
		}
		lastWrittenSeq = msg.Seq
	}

	_, err2 := file.Seek(0, io.SeekStart)
	if err2 != nil {
		_ = file.Truncate(lastOffsetBytes)
		_ = file.Close()
		return err2
	}
	segmentHeader := *s.Header
	segmentHeader.SeqMax = lastWrittenSeq
	err2 = segmentHeader.Serialize(file)
	if err2 != nil {
		_ = file.Truncate(lastOffsetBytes)
		_ = file.Close()
		return err2
	}
	err2 = file.Close()
	if err2 != nil {
		// TODO: got corrupted file at this point
		return err2
	}

	err2 = os.Rename(s.SegmentFilePath, filenamePath)
	if err2 != nil {
		// TODO: got corrupted file at this point
		return err2
	}
	s.SegmentFilePath = filenamePath
	return err
}

func (s *Segment) IsEmpty() bool {
	return s.Header.MessagesCount == 0
}

func (s *Segment) IsPartiallyLoaded() bool {
	return s.PartiallyLoaded
}

func (s *Segment) MessagesSize() int {
	return s.messagesSize
}

func (s *Segment) DeleteSegmentFile() error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.SegmentFilePath != "" {
		return os.Remove(s.SegmentFilePath)
	}
	s.SegmentFilePath = ""
	return nil
}
