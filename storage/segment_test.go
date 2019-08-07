package storage

import (
	"bytes"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/dmitryikh/tube/message"
)

const (
	dataDir = "auto_test_data"
)

func TestMain(m *testing.M) {
	os.MkdirAll(dataDir, 0777)
	code := m.Run()
	os.RemoveAll(dataDir)
	os.Exit(code)
}

func TestEmptySegmentSerialization(t *testing.T) {
	segment := NewActiveSegment("")
	buffer := new(bytes.Buffer)
	err := segment.Serialize(buffer)
	if err != nil {
		t.Fatalf("Serialization error: %s", err)
	}

	newSegment := NewActiveSegment("")
	err = newSegment.Deserialize(buffer)
	if err != nil {
		t.Fatalf("Deserialization error: %s", err)
	}

	if !reflect.DeepEqual(segment, newSegment) {
		t.Fatalf("Different objects")
	}
}

func createMessage(seq uint64, timestamp uint64) *message.Message {
	msg := message.NewMessage()
	msg.Seq = seq
	msg.Payload = []byte("some random message")
	msg.Timestamp = timestamp
	return msg
}

func TestActiveSegmentAppend(t *testing.T) {
	var sFilename string
	messages := []*message.Message{
		createMessage(1, 0),
		createMessage(2, 500),
		createMessage(3, 100500),
		createMessage(5, 100501),
		createMessage(6, 1000000),
	}

	{
		segment := NewActiveSegment("")
		_ = segment.AddMessage(messages[0])
		_ = segment.AddMessage(messages[1])
		_ = segment.AddMessage(messages[2])

		err := segment.SaveToFile(path.Join(dataDir, segmentFilename(segment.Header)))
		if err != nil {
			t.Fatal(err)
		}

		_ = segment.AddMessage(messages[3])
		_ = segment.AddMessage(messages[4])

		sFilename = path.Join(dataDir, segmentFilename(segment.Header))
		err = segment.AppendToFile(sFilename)
		if err != nil {
			t.Fatal(err)
		}
	}

	segment, err := ActiveSegmentFromFile(sFilename)
	if err != nil {
		t.Fatal(err)
	}

	header := &SegmentHeader{
		MessagesCount: 5,
		SeqMin:        1,
		SeqMax:        6,
		TimestampMin:  0,
		TimestampMax:  1000000,
	}

	if !reflect.DeepEqual(header, segment.Header) {
		t.Fatalf("headers are not equal: expected: %v, got: %v", header, segment.Header)
	}

	if !reflect.DeepEqual(messages, segment.Messages) {
		t.Fatalf("messages are not equal: expected: %v, got: %v", header, segment.Header)
	}
}
