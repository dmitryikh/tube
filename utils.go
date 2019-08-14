package tube

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

var (
	crc32Table = crc32.MakeTable(0xEDB88320)
)

type TopicExistsError struct {
	TopicName string
}

func NewTopicExistsError(topicName string) TopicExistsError {
	return TopicExistsError{
		TopicName: topicName,
	}
}

func (e TopicExistsError) Error() string {
	return fmt.Sprintf("topic \"%s\" already exists", e.TopicName)
}

type TopicNotExistError struct {
	TopicName string
}

func NewTopicNotExistError(topicName string) TopicNotExistError {
	return TopicNotExistError{
		TopicName: topicName,
	}
}

func (e TopicNotExistError) Error() string {
	return fmt.Sprintf("topic \"%s\" not exist", e.TopicName)
}

type TopicEmptyError struct {
	TopicName string
}

func NewTopicEmptyError(topicName string) TopicEmptyError {
	return TopicEmptyError{
		TopicName: topicName,
	}
}

func (e TopicEmptyError) Error() string {
	return fmt.Sprintf("topic \"%s\" is empty", e.TopicName)
}

type ConsumerExistsError struct {
	ConsumerID string
}

func NewConsumerExistsError(consumerID string) ConsumerExistsError {
	return ConsumerExistsError{
		ConsumerID: consumerID,
	}
}

func (e ConsumerExistsError) Error() string {
	return fmt.Sprintf("consumer \"%s\" already exists", e.ConsumerID)
}

func MinUint64(v1, v2 uint64) uint64 {
	if v2 > v1 {
		return v1
	}
	return v2
}

func MaxUint64(v1, v2 uint64) uint64 {
	if v2 > v1 {
		return v2
	}
	return v1
}

func DataChecksum(data []byte) uint32 {
	return crc32.Checksum(data, crc32Table)
}

func WriteUint32(writer io.Writer, value uint32) error {
	var buffer [4]byte
	binary.LittleEndian.PutUint32(buffer[:], value)
	n, err := writer.Write(buffer[:])
	if err != nil {
		return err
	}

	if n != 4 {
		return fmt.Errorf("wrote less than 4 bytes")
	}
	return nil
}

func ReadUint32(reader io.Reader) (uint32, error) {
	var buffer [4]byte
	n, err := io.ReadFull(reader, buffer[:])
	if n != len(buffer) {
		return 0, fmt.Errorf("too small array (%d bytes): %s", n, err)
	}
	value := binary.LittleEndian.Uint32(buffer[:])
	return value, nil
}

func IsRegularFile(filePath string) bool {
	info, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func IsDir(dirPath string) bool {
	info, err := os.Stat(dirPath)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

func SecondsToNanoSeconds(seconds int64) uint64 {
	return uint64(seconds) * 1000000000
}

func NanoSecondsToSeconds(nanoSeconds uint64) int64 {
	return int64(nanoSeconds / 1000000000)
}

type PeriodicThread struct {
	waitGroup sync.WaitGroup
	doneChan  chan struct{}
	periodSec int
	jobFunc   func()
}

func NewPeriodicThread(jobFunc func(), periodSec int) *PeriodicThread {
	periodicThread := &PeriodicThread{
		doneChan:  make(chan struct{}, 1),
		periodSec: periodSec,
		jobFunc:   jobFunc,
	}
	periodicThread.waitGroup.Add(1)
	go periodicThread.threadLoop()
	return periodicThread
}

func (t *PeriodicThread) threadLoop() {
	ticker := time.NewTicker(time.Duration(t.periodSec) * time.Second)
Loop:
	for {
		select {
		case <-ticker.C:
			t.jobFunc()
		case <-t.doneChan:
			break Loop
		}
	}
	t.waitGroup.Done()
}

func (t *PeriodicThread) Stop() {
	t.doneChan <- struct{}{}
}

func (t *PeriodicThread) Join() {
	t.waitGroup.Wait()
}
