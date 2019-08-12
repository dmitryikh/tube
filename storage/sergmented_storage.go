package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/dmitryikh/tube"
	"github.com/dmitryikh/tube/message"
)

const (
	defaultHousekeepingPeriodSec = 30
)

var (
	segmentFilePathPattern = regexp.MustCompile(`^(\d+)_(\d+)\.segment$`)
)

type SegmentedStorageConfig struct {
	TopicsDir               string
	SegmentMaxSizeBytes     int
	SegmentMaxSizeMessages  int
	MessageRetentionSec     int
	UnloadMessagesLagSec    int
	FlushingToFilePeriodSec int
	HousekeepingPeriodSec   int
}

type SegmentedTopicStorage struct {
	// last segment - active
	Segments        []*Segment
	lastTimeFlushed time.Time
	consumedSeq     uint64
}

func NewSegmentedTopicStorage() (*SegmentedTopicStorage, error) {
	segmentedTopicStorage := &SegmentedTopicStorage{
		Segments:        []*Segment{NewSegment()},
		lastTimeFlushed: time.Now(),
	}
	return segmentedTopicStorage, nil
}

func (s *SegmentedTopicStorage) ActiveSegment() *Segment {
	return s.Segments[len(s.Segments)-1]
}

type SegmentedStorage struct {
	Topics           map[string]*SegmentedTopicStorage
	config           *SegmentedStorageConfig
	houseKeepingDone chan struct{}
	lock             sync.RWMutex
}

func NewSegmentedStorage(config *SegmentedStorageConfig) (*SegmentedStorage, error) {
	segmentedStorage := &SegmentedStorage{
		Topics:           make(map[string]*SegmentedTopicStorage),
		config:           config,
		houseKeepingDone: make(chan struct{}),
	}

	topicsDir := segmentedStorage.config.TopicsDir
	if !tube.IsDir(topicsDir) {
		err := os.Mkdir(topicsDir, 0777)
		if err != nil {
			return nil, err
		}
	} else {
		files, err := ioutil.ReadDir(topicsDir)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			if file.IsDir() && isTopicDir(path.Join(topicsDir, file.Name())) {
				err := segmentedStorage.OpenTopic(file.Name())
				if err != nil {
					return nil, err
				}
			}
		}
		// Каждая папка, в которой есть файл с маской сегмента - топик
		// Загружаем последний сегмент, как активный, остальные - только header
		// 2. Прошерстить директорию на предмент сохраненных сегментов и загрузить их
		// 3. Крайний загружаем как ActiveSegment, а предыдущие как Segment (только header)
	}

	go housekeeping(segmentedStorage)

	return segmentedStorage, nil
}

func (s *SegmentedStorage) AddTopic(name string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, isFound := s.Topics[name]; isFound {
		return fmt.Errorf("topic '%s' is already exist", name)
	}
	topicDir := path.Join(s.config.TopicsDir, name)
	err := os.Mkdir(topicDir, 0777)
	if err != nil {
		return err
	}

	topicStorage, err := NewSegmentedTopicStorage()
	if err != nil {
		return err
	}

	activeSegment := topicStorage.ActiveSegment()
	err = activeSegment.SaveToFile(path.Join(topicDir, segmentFilename(activeSegment.Header)))
	if err != nil {
		return err
	}

	s.Topics[name] = topicStorage
	return nil
}

func (s *SegmentedStorage) RemoveTopic(name string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, isFound := s.Topics[name]; !isFound {
		return fmt.Errorf("topic '%s' doesn't exist", name)
	}
	topicDir := path.Join(s.config.TopicsDir, name)
	err := os.RemoveAll(topicDir)
	if err != nil {
		return err
	}

	delete(s.Topics, name)
	return nil
}

func (s *SegmentedStorage) AddMessage(topicName string, msg *message.Message) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var err error
	topicStorage, isFound := s.Topics[topicName]
	if !isFound {
		return fmt.Errorf("topic '%s' doesn't exist", topicName)
	}
	err = topicStorage.ActiveSegment().AddMessage(msg)
	// TODO: change segment in case of overflow
	// need to Lock() to write
	return err
}

func (s *SegmentedStorage) GetNextMessage(topicName string, seq uint64) (*message.Message, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	topicStorage, isFound := s.Topics[topicName]
	if !isFound {
		return nil, fmt.Errorf("topic '%s' doesn't exist", topicName)
	}
	foundSegmentIdx := -1
	for i := 0; i < len(topicStorage.Segments); i++ {
		if topicStorage.Segments[i].Header.SeqMax < seq {
			foundSegmentIdx = i
			break
		}
	}
	if foundSegmentIdx == -1 {
		seqMax := topicStorage.Segments[len(topicStorage.Segments)-1].Header.SeqMax
		return nil, fmt.Errorf("seq >= seqMax (%d >= %d)", seq, seqMax)
	}

	foundSegment := topicStorage.Segments[foundSegmentIdx]
	if foundSegment.IsPartiallyLoaded() {
		// TODO: need to acquire write lock here
		err := foundSegment.LoadMessages()
		if err != nil {
			return nil, fmt.Errorf("error while loading segment %s: %s", foundSegment.SegmentFilePath, err)
		}
	}
	return foundSegment.GetNextMessage(seq)
}

func (s *SegmentedStorage) GetLastMessage(topicName string) (*message.Message, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	topicStorage, isFound := s.Topics[topicName]
	if !isFound {
		return nil, fmt.Errorf("topic '%s' doesn't exist", topicName)
	}
	activeSegment := topicStorage.ActiveSegment()
	if activeSegment.IsEmpty() {
		nSegments := len(topicStorage.Segments)
		if nSegments > 1 {
			prevSegment := topicStorage.Segments[nSegments-2]
			if prevSegment.IsPartiallyLoaded() {
				// TODO: need to acquire write lock here
				err := prevSegment.LoadMessages()
				if err != nil {
					return nil, fmt.Errorf("error while loading segment %s: %s", prevSegment.SegmentFilePath, err)
				}
			}
			return prevSegment.GetLastMessage()
		}
		return nil, fmt.Errorf("no messages in topic \"%s\"", topicName)
	}

	return activeSegment.GetLastMessage()
}

func (s *SegmentedStorage) Shutdown() error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var firstError error
	for topic, topicStorage := range s.Topics {
		activeSegment := topicStorage.ActiveSegment()
		segmentPath := path.Join(s.config.TopicsDir, topic, segmentFilename(activeSegment.Header))

		// do nothing if min/max seqs are not changed
		if activeSegment.SegmentFilePath != "" {
			filename := path.Base(activeSegment.SegmentFilePath)
			fileMinSeq, fileMaxSeq := minMaxSeqsFromSegmentFilename(filename)
			if fileMinSeq == activeSegment.Header.SeqMin && fileMaxSeq == activeSegment.Header.SeqMax {
				return nil
			}
		}

		err := activeSegment.SaveToFile(segmentPath)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func (s *SegmentedStorage) tryCreateNextActiveSegment(topicName string) error {
	// TODO
	return nil
}

func (s *SegmentedStorage) tryDeleteOldSegments(topicName string) error {
	// TODO
	return nil
}

func (s *SegmentedStorage) tryUnloadMessages(topicName string) error {
	// TODO
	return nil
}

func (s *SegmentedStorage) OpenTopic(topicName string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// Крайний загружаем как ActiveSegment, а предыдущие как Segment (только header)
	topicPath := path.Join(s.config.TopicsDir, topicName)
	segments, err := listSegments(topicPath)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return fmt.Errorf("Directory %s does not contain segments", topicPath)
	}

	n := len(segments)
	activeSegmentFilename := segmentFilename(segments[n-1].Header)

	activeSegment, err := SegmentFromFile(path.Join(topicPath, activeSegmentFilename), true)
	if err != nil {
		return err
	}

	topicStorage := &SegmentedTopicStorage{
		Segments: segments,
	}
	topicStorage.Segments[len(topicStorage.Segments)-1] = activeSegment

	s.Topics[topicName] = topicStorage
	return nil
}

func listSegments(topicPath string) ([]*Segment, error) {
	files, err := ioutil.ReadDir(topicPath)
	if err != nil {
		return nil, err
	}
	segments := make([]*Segment, 0)

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		segment, err := SegmentFromFile(path.Join(topicPath, file.Name()), false)
		if err != nil {
			return nil, err
		}

		segments = append(segments, segment)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Header.SeqMin < segments[j].Header.SeqMin
	})
	// TODO: may be check on SeqMin & SeqMax non-overlapping ?

	return segments, nil
}

func housekeeping(s *SegmentedStorage) {
	if s.config.HousekeepingPeriodSec < defaultHousekeepingPeriodSec {
		s.config.HousekeepingPeriodSec = defaultHousekeepingPeriodSec
	}
	ticker := time.NewTicker(time.Duration(s.config.HousekeepingPeriodSec) * time.Second)
	for {
		select {
		case <-ticker.C:

		case <-s.houseKeepingDone:
			break
		}
	}
}

func housekeepingInternal(s *SegmentedStorage) {
	// 1. Delete old segments (based on consumedSeq & retention)
	// 2. Flush active segment to the disk
	// 3. Unload messages from unused long time segments
	now := time.Now().Unix()
	s.lock.RLock()
	segmentsToDelete := make(map[string][]int)
	for topicName, topic := range s.Topics {
		if s.config.FlushingToFilePeriodSec != 0 &&
			(topic.lastTimeFlushed.Unix()+int64(s.config.FlushingToFilePeriodSec) < now) {
			// time to flush
			activeSegment := topic.ActiveSegment()
			err := activeSegment.AppendToFile(segmentFilename(activeSegment.Header))
			if err != nil {
				// TODO: log the error
			}
			topic.lastTimeFlushed = time.Now()
		}

		for idx, segment := range topic.Segments {
			if idx == len(topic.Segments)-1 {
				// do not unload active segment
				continue
			}

			if topic.consumedSeq >= segment.Header.SeqMax {
				// segment consumed by all consumers, then can be deleted if ok with retention policy
				timeRetentionPass := s.config.MessageRetentionSec > 0 &&
					(int64(s.config.MessageRetentionSec)+tube.NanoSecondsToSeconds(segment.Header.TimestampMax) < now)

				if timeRetentionPass {
					segmentsToDelete[topicName] = append(segmentsToDelete[topicName], idx)
					// do not perform futher housekeep as soon the segment will be deleted
					continue
				}
			}

			if !segment.IsPartiallyLoaded() && (segment.LastMessageRead.Unix()+int64(s.config.UnloadMessagesLagSec) < now) {
				err := segment.UnloadMessages()
				if err != nil {
					// TODO: log the error
				}

			}
		}
	}

	s.lock.RUnlock()
	if len(segmentsToDelete) > 0 {
		s.lock.Lock()
		defer s.lock.Unlock()
		for topicName, segments := range segmentsToDelete {
			topic, isFound := s.Topics[topicName]
			if !isFound {
				// topic iteseld was deleted ..
				continue
			}

			segmentsSet := make(map[int]struct{})
			for _, segmentIdx := range segments {
				segmentsSet[segmentIdx] = struct{}{}
			}
			newSegments := make([]*Segment, 0, len(topic.Segments))
			for idx, segment := range topic.Segments {
				if _, isFound := segmentsSet[idx]; isFound {
					err := segment.DeleteSegmentFile()
					if err != nil {
						// TODO: log the error
					}
				} else {
					newSegments = append(newSegments, segment)
				}
			}
			topic.Segments = newSegments
		}
	}
}
