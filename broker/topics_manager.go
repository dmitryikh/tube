package broker

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/dmitryikh/tube"
	"github.com/dmitryikh/tube/message"
	"github.com/dmitryikh/tube/storage"
)

const (
	topicsMetadataFilename string = "topics.bin"
	topicsDirName          string = "topics"
)

func NewSegmentedStorageConfig(config *Config, topicsDir string) *storage.SegmentedStorageConfig {
	return &storage.SegmentedStorageConfig{
		TopicsDir:               topicsDir,
		SegmentMaxSizeBytes:     config.SegmentMaxSizeBytes,
		SegmentMaxSizeMessages:  config.SegmentMaxSizeMessages,
		MessageRetentionSec:     config.MessageRetentionSec,
		UnloadMessagesLagSec:    config.UnloadMessagesLagSec,
		FlushingToFilePeriodSec: config.StorageFlushingToFilePeriodSec,
		HousekeepingPeriodSec:   config.StorageHousekeepingPeriodSec,
	}
}

type TopicsManagerConfig struct {
	DataDir string
}

func NewTopicsManagerConfig(config *Config) *TopicsManagerConfig {
	return &TopicsManagerConfig{
		DataDir: config.DataDir,
	}
}

// TopicsManager - CRUD над топиками, Add/Get сообщений, хранение сообщений
type TopicsManager struct {
	mutex   sync.Mutex
	config  *TopicsManagerConfig
	Topics  map[string]Topic
	Storage storage.Storage
}

func NewTopicManager(config *Config) (*TopicsManager, error) {
	manager := &TopicsManager{}
	manager.config = NewTopicsManagerConfig(config)

	// TODO: create DataDir if not exists
	err := os.MkdirAll(config.DataDir, 0777)
	if err != nil {
		return nil, err
	}
	metadataFilePath := path.Join(config.DataDir, topicsMetadataFilename)
	if !tube.IsRegularFile(metadataFilePath) {
		file, err := os.Create(metadataFilePath)
		if err != nil {
			return nil, err
		}
		err = manager.SerializeMetadata(file)
		if err != nil {
			file.Close()
			return nil, err
		}
		err = file.Close()
		if err != nil {
			return nil, err
		}
	}
	file, err := os.Open(metadataFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	err = manager.DeserializeMetadata(file)
	if err != nil {
		return nil, err
	}

	topicsDirPath := path.Join(config.DataDir, topicsDirName)
	storageConfig := NewSegmentedStorageConfig(config, topicsDirPath)
	topicsStorage, err := storage.NewSegmentedStorage(storageConfig)
	if err != nil {
		return nil, err
	}
	manager.Storage = topicsStorage
	return manager, nil
}

func (m *TopicsManager) Check() error {
	return nil
}

func (m *TopicsManager) SerializeMetadata(writer io.Writer) error {
	// first 4 bytes - reserved for version
	const version = 0
	err := tube.WriteUint32(writer, version)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(writer)
	err = encoder.Encode(m.Topics)
	if err != nil {
		return err
	}
	return nil
}

func (m *TopicsManager) DeserializeMetadata(reader io.Reader) error {
	// first 4 bytes - reserved for version
	version, err := tube.ReadUint32(reader)
	if err != nil {
		return err
	}
	if version == 0 {
		decoder := gob.NewDecoder(reader)
		err := decoder.Decode(&m.Topics)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unsupported version %d", version)
	}
	return nil
}

func (m *TopicsManager) AddTopic(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, isFound := m.Topics[name]; isFound {
		return fmt.Errorf("topic '%s' is already exist", name)
	}
	err := m.Storage.AddTopic(name)
	if err != nil {
		return err
	}

	m.Topics[name] = NewTopic()
	return nil
}

func (m *TopicsManager) RemoveTopic(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, isFound := m.Topics[name]; !isFound {
		return fmt.Errorf("topic '%s' doesn't exist", name)
	}
	err := m.Storage.RemoveTopic(name)
	if err != nil {
		return err
	}
	delete(m.Topics, name)
	return nil
}

func (m *TopicsManager) GetConsumedSeq(topicsName string) (uint64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	topic, isFound := m.Topics[topicsName]
	if !isFound {
		return 0, fmt.Errorf("topic '%s' doesn't exist", topicsName)
	}
	return topic.ConsumedSeq, nil
}

func (m *TopicsManager) SetConsumedSeq(topicsName string, consumedSeq uint64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if topic, isFound := m.Topics[topicsName]; isFound {
		if topic.ConsumedSeq < consumedSeq {
			topic.ConsumedSeq = consumedSeq
		}
	}
}

func (m *TopicsManager) AddMessage(topicName string, msg *message.Message) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.Storage.AddMessage(topicName, msg)
}

func (m *TopicsManager) GetNextMessage(topicName string, seq uint64) (*message.Message, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.Storage.GetNextMessage(topicName, seq)
}

func (m *TopicsManager) GetLastMessage(topicName string) (*message.Message, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.Storage.GetLastMessage(topicName)
}

func (m *TopicsManager) Shutdown() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	err := m.Storage.Shutdown()
	if err != nil {
		return err
	}

	metadataFilePath := path.Join(m.config.DataDir, topicsMetadataFilename)
	file, err := os.Create(metadataFilePath)
	if err != nil {
		return err
	}
	err = m.SerializeMetadata(file)
	if err != nil {
		file.Close()
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}
