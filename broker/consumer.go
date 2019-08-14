package broker

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/dmitryikh/tube"
)

const (
	consumerRegistryFilename string = "consumers.bin"
)

// type ConsumerStatus int
//
// const (
// 	ConsumerOffline ConsumerStatus = 0
// 	ConsumerOnline  ConsumerStatus = 1
// 	ConsumerLost    ConsumerStatus = 2
// )
//
// func (s ConsumerStatus) String() string {
// 	names := [...]string{
// 		"Offline",
// 		"Online",
// 		"Lost",
// 	}
// 	if s < ConsumerOffline || s > ConsumerLost {
// 		return "Unknown"
// 	}
// 	return names[s]
// }

type ConsumerRecord struct {
	Seqs map[string]uint64
	// Status ConsumerStatus
	// LastCall time.Time
}

func (r *ConsumerRecord) Copy() *ConsumerRecord {
	consumerRecord := &ConsumerRecord{
		Seqs: make(map[string]uint64),
		// Status: r.Status,
	}
	for topicName, seq := range r.Seqs {
		consumerRecord.Seqs[topicName] = seq
	}
	return consumerRecord
}

type ConsumersRegistry struct {
	Consumers map[string]ConsumerRecord
	lock      sync.Mutex
}

func NewConsumersRegistry() *ConsumersRegistry {
	return &ConsumersRegistry{
		Consumers: make(map[string]ConsumerRecord),
	}
}

func ConsumerRegistryFromFile(fileNamePath string) (*ConsumersRegistry, error) {
	file, err := os.Open(fileNamePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	bufReader := bufio.NewReader(file)
	registry := NewConsumersRegistry()
	err = registry.Deserialize(bufReader)
	if err != nil {
		return nil, err
	}
	return registry, err
}

func (s *ConsumersRegistry) SaveToFile(fileNamePath string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
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

	return nil
}

func (r *ConsumersRegistry) Serialize(writer io.Writer) error {
	// first 4 bytes - reserved for version
	version := uint32(0)
	err := tube.WriteUint32(writer, version)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(writer)
	return encoder.Encode(r.Consumers)
}

func (r *ConsumersRegistry) Deserialize(reader io.Reader) error {
	// first 4 bytes - reserved for version
	version, err := tube.ReadUint32(reader)
	if err != nil {
		return err
	}
	if version == 0 {
		decoder := gob.NewDecoder(reader)
		r.Consumers = make(map[string]ConsumerRecord)
		err := decoder.Decode(&r.Consumers)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unsupported version %d", version)
	}
	return nil
}

// func (r *ConsumersRegistry) SetConsumerOnline(consumerName string) error {
// 	r.lock.Lock()
// 	defer r.lock.Unlock()
// 	consumer, isFound := r.Consumers[consumerName]
// 	if !isFound {
// 		return fmt.Errorf("Consumer \"%s\" doesn't exist", consumerName)
// 	}
// 	if consumer.Status == ConsumerOnline {
// 		return fmt.Errorf("Consumer \"%s\" is already online", consumerName)
// 	}
// 	consumer.Status = ConsumerOnline
// 	return nil
// }
//
// func (r *ConsumersRegistry) SetConsumerOffline(consumerName string) error {
// 	r.lock.Lock()
// 	defer r.lock.Unlock()
// 	consumer, isFound := r.Consumers[consumerName]
// 	if !isFound {
// 		return fmt.Errorf("Consumer \"%s\" doesn't exist", consumerName)
// 	}
// 	consumer.Status = ConsumerOffline
// 	return nil
// }
//
// func (r *ConsumersRegistry) SetConsumersLost(timeoutSec int) []string {
// 	r.lock.Lock()
// 	defer r.lock.Unlock()
//
// 	consumersGetLost := make([]string, 0)
//
// 	now := time.Now().Unix()
// 	for consumerName, consumerRecord := range r.Consumers {
// 		if consumerRecord.LastCall.Unix()+int64(timeoutSec) < now {
// 			consumersGetLost = append(consumersGetLost, consumerName)
// 			consumerRecord.Status = ConsumerLost
// 		}
// 	}
// 	return consumersGetLost
// }

// func (r *ConsumersRegistry) ConsumerStatus(consumerName string) (ConsumerStatus, error) {
// 	r.lock.Lock()
// 	defer r.lock.Unlock()
// 	consumer, isFound := r.Consumers[consumerName]
// 	if !isFound {
// 		return 0, fmt.Errorf("Consumer \"%s\" doesn't exist", consumerName)
// 	}
// 	return consumer.Status, nil
// }

func (r *ConsumersRegistry) ConsumerRecords() map[string]*ConsumerRecord {
	r.lock.Lock()
	defer r.lock.Unlock()
	consumersData := make(map[string]*ConsumerRecord)
	for consumerName, consumerRecord := range r.Consumers {
		consumersData[consumerName] = consumerRecord.Copy()
	}
	return consumersData
}

func (r *ConsumersRegistry) AddConsumer(name string, seqSet tube.SeqSet) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, isFound := r.Consumers[name]; isFound {
		return tube.NewConsumerExistsError(name)
	}
	// TODO: adjust seqSet to GetAllTopicsSeqs()
	seqSetCopy := make(map[string]uint64)
	for k, v := range seqSet {
		seqSetCopy[k] = v
	}
	r.Consumers[name] = ConsumerRecord{
		Seqs: seqSetCopy,
		// Status: ConsumerOffline,
	}
	return nil
}

func (r *ConsumersRegistry) DeleteConsumer(name string) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, isFound := r.Consumers[name]; !isFound {
		return fmt.Errorf("Consumer \"%s\" not exist", name)
	}
	delete(r.Consumers, name)
	return nil
}

func (r *ConsumersRegistry) SetConsumedSeqs(consumerName string, seqs map[ /*topic*/ string] /*seq*/ uint64) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	consumerRecord, isFound := r.Consumers[consumerName]
	if !isFound {
		return fmt.Errorf("Consumer \"%s\" doesn't exists", consumerName)
	}

	// if consumerRecord.Status != ConsumerOnline {
	// 	return fmt.Errorf("Consumer \"%s\" not online", consumerName)
	// }

	// consumerRecord.LastCall = time.Now()

	// check before assigment
	for topicName, _ := range seqs {
		if _, isFound := consumerRecord.Seqs[topicName]; !isFound {
			return fmt.Errorf("Topic \"%s\" not belong to consumer \"%s\"", topicName, consumerName)
		}
	}

	for topicName, newSeq := range seqs {
		oldSeq := consumerRecord.Seqs[topicName]
		if newSeq > oldSeq {
			consumerRecord.Seqs[topicName] = newSeq
		}
	}
	return nil
}

func (r *ConsumersRegistry) GetAllTopicsSeqs() tube.SeqSet {
	r.lock.Lock()
	defer r.lock.Unlock()
	seqs := tube.NewSeqSet()
	for _, consumerRecord := range r.Consumers {
		seqs.UpdateMin(consumerRecord.Seqs)
	}
	return seqs
}
