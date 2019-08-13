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
	log "github.com/sirupsen/logrus"
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

func NewSegmentedTopicStorage() *SegmentedTopicStorage {
	return &SegmentedTopicStorage{
		Segments:        []*Segment{NewSegment()},
		lastTimeFlushed: time.Now(),
		consumedSeq:     0,
	}
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
	log.Info("Creating SegmentedStorage..")
	segmentedStorage := &SegmentedStorage{
		Topics:           make(map[string]*SegmentedTopicStorage),
		config:           config,
		houseKeepingDone: make(chan struct{}),
	}

	topicsDir := segmentedStorage.config.TopicsDir
	if !tube.IsDir(topicsDir) {
		log.Infof("Creating SegmentedStorage: create topics dir \"%s\"", topicsDir)
		err := os.Mkdir(topicsDir, 0777)
		if err != nil {
			return nil, err
		}
	} else {
		log.Infof("Creating SegmentedStorage: topics dir is \"%s\"", topicsDir)
		files, err := ioutil.ReadDir(topicsDir)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			if file.IsDir() && isTopicDir(path.Join(topicsDir, file.Name())) {
				log.Infof("Creating SegmentedStorage: open topic \"%s\"", file.Name())
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

	log.Info("Creating SegmentedStorage.. Done")
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

	topicStorage := NewSegmentedTopicStorage()

	// activeSegment := topicStorage.ActiveSegment()
	// err = activeSegment.SaveToFile(path.Join(topicDir, segmentFilename(activeSegment.Header)))
	// if err != nil {
	// 	return err
	// }

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

func (s *SegmentedStorage) AddMessages(topicName string, messages []*message.Message) error {
	s.lock.RLock()
	var err error
	topicStorage, isFound := s.Topics[topicName]
	if !isFound {
		s.lock.RUnlock()
		return fmt.Errorf("topic '%s' doesn't exist", topicName)
	}
	activeSegment := topicStorage.ActiveSegment()
	err = activeSegment.AddMessages(messages)
	if err != nil {
		s.lock.RUnlock()
		return err
	}
	s.lock.RUnlock()
	if activeSegment.MessagesSize() >= s.config.SegmentMaxSizeBytes ||
		len(activeSegment.Messages) >= s.config.SegmentMaxSizeMessages {
		// initiate new segment
		s.lock.Lock()
		defer s.lock.Unlock()

		segmentPath := path.Join(s.config.TopicsDir, topicName, segmentFilename(activeSegment.Header))
		log.WithFields(log.Fields{
			"topic": topicName,
		}).Infof("Spin new segment after %s", segmentFilename(activeSegment.Header))
		err := activeSegment.AppendToFile(segmentPath)
		if err != nil {
			return err
		}
		topicStorage.Segments = append(topicStorage.Segments, NewSegment())
	}
	return err
}

func (s *SegmentedStorage) GetMessages(topicName string, seq uint64, maxBatch uint32) ([]*message.Message, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	topicStorage, isFound := s.Topics[topicName]
	if !isFound {
		return nil, fmt.Errorf("topic '%s' doesn't exist", topicName)
	}

	iter := NewSegmentedIterator(topicStorage.Segments)
	err := iter.Seek(seq)
	if err != nil {
		return nil, err
	}
	messages := make([]*message.Message, 0)
	for uint32(len(messages)) < maxBatch {
		msg, err := iter.Next()
		if err != nil {
			if _, ok := err.(StopIterationError); ok {
				return messages, nil
			}
			return nil, err
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

func (s *SegmentedStorage) GetLastMessage(topicName string) (*message.Message, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	topicStorage, isFound := s.Topics[topicName]
	if !isFound {
		return nil, fmt.Errorf("topic '%s' doesn't exist", topicName)
	}
	iter := NewSegmentedIterator(topicStorage.Segments)
	err := iter.SeekEnd()
	if err != nil {
		return nil, err
	}
	return iter.Next()
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

		// TODO: change to AppendToFile?
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

func (s *SegmentedStorage) SetConsumedSeq(topicName string, seq uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if topic, isFound := s.Topics[topicName]; isFound {
		if topic.consumedSeq < seq {
			topic.consumedSeq = seq
		}
	}
}

func (s *SegmentedStorage) GetStoredSeq(topicName string) (uint64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if topic, isFound := s.Topics[topicName]; isFound {
		activeSegment := topic.ActiveSegment()
		if activeSegment.SegmentFilePath != "" {
			_, maxStoredSeq := minMaxSeqsFromSegmentFilename(activeSegment.SegmentFilePath)
			return maxStoredSeq, nil
		}
		n := len(topic.Segments)
		if n > 1 {
			preLastSegment := topic.Segments[n-2]
			_, maxStoredSeq := minMaxSeqsFromSegmentFilename(preLastSegment.SegmentFilePath)
			return maxStoredSeq, nil
		}
	}
	// return 0, fmt.Errorf("can't find last stored seq fro topic \"%s\"", topicName)
	return 0, nil
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
	if s.config.HousekeepingPeriodSec < 1 {
		s.config.HousekeepingPeriodSec = defaultHousekeepingPeriodSec
	}
	log.WithField("period", s.config.HousekeepingPeriodSec).
		Info("Launch housekeeping thread")
	ticker := time.NewTicker(time.Duration(s.config.HousekeepingPeriodSec) * time.Second)
	for {
		select {
		case <-ticker.C:
			housekeepingInternal(s)
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
	log.Trace("Do housekeeping..")
	segmentsToDelete := make(map[string][]int)
	for topicName, topic := range s.Topics {
		if s.config.FlushingToFilePeriodSec != 0 &&
			(topic.lastTimeFlushed.Unix()+int64(s.config.FlushingToFilePeriodSec) < now) {
			// time to flush
			activeSegment := topic.ActiveSegment()
			newSegmentFilename := segmentFilename(activeSegment.Header)
			log.WithFields(log.Fields{
				"topic":   topicName,
				"segment": newSegmentFilename,
			}).Info("Flushing to disk")
			segmentPath := path.Join(s.config.TopicsDir, topicName, newSegmentFilename)
			err := activeSegment.AppendToFile(segmentPath)
			if err != nil {
				log.WithFields(log.Fields{
					"topic":   topicName,
					"segment": newSegmentFilename,
				}).Errorf("Flushing to disk failed: %s", err)
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
					log.WithFields(log.Fields{
						"topic":   topicName,
						"segment": segmentFilename(segment.Header),
					}).Trace("Marked to delete")
					// do not perform futher housekeep as soon the segment will be deleted
					continue
				}
			}

			if !segment.IsPartiallyLoaded() && (segment.LastMessageRead.Unix()+int64(s.config.UnloadMessagesLagSec) < now) {
				log.WithFields(log.Fields{
					"topic":   topicName,
					"segment": segmentFilename(segment.Header),
				}).Info("Unloading segment")
				err := segment.UnloadMessages()
				if err != nil {
					log.WithFields(log.Fields{
						"topic":   topicName,
						"segment": segmentFilename(segment.Header),
					}).Errorf("Failed to unload messages: %s", err)
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
				// topic itself was deleted ..
				log.WithField("topic", topicName).
					Info("Topic was deleted while housekeeping")
				continue
			}

			segmentsSet := make(map[int]struct{})
			for _, segmentIdx := range segments {
				segmentsSet[segmentIdx] = struct{}{}
			}
			newSegments := make([]*Segment, 0, len(topic.Segments))
			for idx, segment := range topic.Segments {
				if _, isFound := segmentsSet[idx]; isFound {
					log.WithFields(log.Fields{
						"topic":   topicName,
						"segment": segmentFilename(segment.Header),
					}).Info("Delete segment")
					err := segment.DeleteSegmentFile()
					if err != nil {
						log.WithFields(log.Fields{
							"topic":   topicName,
							"segment": segmentFilename(segment.Header),
						}).Errorf("Failed to delete segment: %s", err)
					}
				} else {
					newSegments = append(newSegments, segment)
				}
			}
			topic.Segments = newSegments
		}
	}
	log.Trace("Do housekeeping.. Done")
}
