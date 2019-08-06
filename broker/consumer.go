package broker

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/dmitryikh/tube"
)

type ConsumerStatus int

const (
	ConsumerOffline ConsumerStatus = 0
	ConsumerOnline  ConsumerStatus = 1
	ConsumerLost    ConsumerStatus = 2
)

func (s ConsumerStatus) String() string {
	names := [...]string{
		"Offline",
		"Online",
		"Lost",
	}
	if s < ConsumerOffline || s > ConsumerLost {
		return "Unknown"
	}
	return names[s]
}

type ConsumerRecord struct {
	Seqs   map[string]uint64
	Status ConsumerStatus
}

type ConsumersRegistry struct {
	Consumers map[string]ConsumerRecord
	mutex     sync.Mutex
}

func (r *ConsumersRegistry) Serialize() ([]byte, error) {
	// first 4 bytes - reserved for version
	version := uint32(0)
	buffer := new(bytes.Buffer)
	var versionBuffer [4]byte
	binary.LittleEndian.PutUint32(versionBuffer[:], version)
	_, err := buffer.Write(versionBuffer[:])
	if err != nil {
		return nil, err
	}
	encoder := gob.NewEncoder(buffer)
	err = encoder.Encode(r.Consumers)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (r *ConsumersRegistry) Deserialize(data []byte) error {
	// first 4 bytes - reserved for version
	if len(data) <= 4 {
		return fmt.Errorf("too small array (%d bytes)", len(data))
	}
	version := binary.LittleEndian.Uint32(data)
	if version == 0 {
		buffer := bytes.NewBuffer(data[4:])
		decoder := gob.NewDecoder(buffer)
		err := decoder.Decode(&r.Consumers)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unsupported version %d", version)
	}
	return nil
}

func (r *ConsumersRegistry) AddConsumer(name string, seqSet tube.SeqSet) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, isFound := r.Consumers[name]; isFound {
		return fmt.Errorf("Consumer '%s' already exists", name)
	}
	seqSetCopy := make(map[string]uint64)
	for k, v := range seqSet {
		seqSetCopy[k] = v
	}
	r.Consumers[name] = ConsumerRecord{
		Seqs:   seqSetCopy,
		Status: ConsumerOffline,
	}
	return nil
}

func (r *ConsumersRegistry) DeleteConsumer(name string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, isFound := r.Consumers[name]; !isFound {
		return fmt.Errorf("Consumer '%s' doesn't exists", name)
	}
	delete(r.Consumers, name)
	return nil
}

func (r *ConsumersRegistry) GetAllTopicsSeqs() tube.SeqSet {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	seqs := tube.NewSeqSet()
	for _, consumerRecord := range r.Consumers {
		seqs.UpdateMin(consumerRecord.Seqs)
	}
	return seqs
}
