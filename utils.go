package tube

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
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
