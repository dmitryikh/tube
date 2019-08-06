package storage

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"

	"github.com/dmitryikh/tube"
	"github.com/dmitryikh/tube/message"
)

var (
	segmentFilePathPattern = regexp.MustCompile(`^(\d+)_(\d+)\.segment$`)
)

type SegmentedStorageConfig struct {
	TopicsDir              string
	SegmentMaxSizeBytes    int
	SegmentMaxSizeMessages int
}

type SegmentedTopicStorage struct {
	ActiveSegment *ActiveSegment
	Segments      []*Segment
}

func NewSegmentedTopicStorage() (*SegmentedTopicStorage, error) {
	segmentedTopicStorage := &SegmentedTopicStorage{
		ActiveSegment: NewActiveSegment(""),
		Segments:      make([]*Segment, 0),
	}
	return segmentedTopicStorage, nil
}

type SegmentedStorage struct {
	Topics map[string]*SegmentedTopicStorage
	config *SegmentedStorageConfig
}

func NewSegmentedStorage(config *SegmentedStorageConfig) (*SegmentedStorage, error) {
	segmentedStorage := &SegmentedStorage{
		Topics: make(map[string]*SegmentedTopicStorage),
		config: config,
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

	return segmentedStorage, nil
}

func (s *SegmentedStorage) AddTopic(name string) error {
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

	s.Topics[name] = topicStorage
	return nil
}

func (s *SegmentedStorage) RemoveTopic(name string) error {
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
	var err error
	topicStorage, isFound := s.Topics[topicName]
	if !isFound {
		return fmt.Errorf("topic '%s' doesn't exist", topicName)
	}
	err = topicStorage.ActiveSegment.AddMessage(msg)
	return err
}

func (s *SegmentedStorage) GetNextMessage(topicName string, seq uint64) (*message.Message, error) {
	topicStorage, isFound := s.Topics[topicName]
	if !isFound {
		return nil, fmt.Errorf("topic '%s' doesn't exist", topicName)
	}
	if seq >= topicStorage.ActiveSegment.Header.SeqMax {
		return nil, fmt.Errorf("seq >= seqMax (%d > %d)", seq, topicStorage.ActiveSegment.Header.SeqMax)
	}

	if seq < topicStorage.ActiveSegment.Header.SeqMin {
		// TODO
		return nil, fmt.Errorf("seq < seqMin (%d < %d) (not implemented yet)", seq, topicStorage.ActiveSegment.Header.SeqMin)
	}
	return topicStorage.ActiveSegment.GetNextMessage(seq)
}

func (s *SegmentedStorage) GetLastMessage(topicName string) (*message.Message, error) {
	topicStorage, isFound := s.Topics[topicName]
	if !isFound {
		return nil, fmt.Errorf("topic '%s' doesn't exist", topicName)
	}
	l := len(topicStorage.ActiveSegment.Messages)
	if l == 0 {
		// TODO: what if just dump active segment to disk and start new one?
		return nil, fmt.Errorf("no messages in active segment (topic %s)", topicName)
	}

	return topicStorage.ActiveSegment.Messages[l-1], nil
}

func (s *SegmentedStorage) Shutdown() error {
	var firstError error
	for topic, topicStorage := range s.Topics {
		segmentPath := path.Join(s.config.TopicsDir, topic, segmentFilename(topicStorage.ActiveSegment.Header))
		err := topicStorage.ActiveSegment.SaveToFile(segmentPath)
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

func (s *SegmentedStorage) OpenTopic(topicName string) error {
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
	activeSegmentFilepath := path.Join(topicPath, activeSegmentFilename)
	fd, err := os.Open(activeSegmentFilepath)
	if err != nil {
		return err
	}
	defer fd.Close()
	bufReader := bufio.NewReader(fd)
	activeSegment := NewActiveSegment(activeSegmentFilepath)
	err = activeSegment.Deserialize(bufReader)
	if err != nil {
		return err
	}

	topicStorage := &SegmentedTopicStorage{
		ActiveSegment: activeSegment,
		Segments:      segments[0 : n-1],
	}

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
		fd, err := os.Open(path.Join(topicPath, file.Name()))
		if err != nil {
			return nil, err
		}
		segment := NewSegment()
		err = segment.Deserialize(fd)
		fd.Close()
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

func segmentFilename(header *SegmentHeader) string {
	return fmt.Sprintf("%d_%d.segment", header.SeqMin, header.SeqMax)
}

func isSegmentFile(filePath string) bool {
	return segmentFilePathPattern.MatchString(filePath)
}

func isTopicDir(dirPath string) bool {
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return false
	}
	for _, file := range files {
		if isSegmentFile(file.Name()) {
			return true
		}
	}
	return false
}
