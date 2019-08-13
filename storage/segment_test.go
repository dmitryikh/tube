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
	segment := NewSegment()
	buffer := new(bytes.Buffer)
	err := segment.Serialize(buffer)
	if err != nil {
		t.Fatalf("Serialization error: %s", err)
	}

	newSegment := NewSegment()
	err = newSegment.Deserialize(buffer, true)
	if err != nil {
		t.Fatalf("Deserialization error: %s", err)
	}
	// make times equal for comparison
	newSegment.LastMessageRead = segment.LastMessageRead

	if !reflect.DeepEqual(segment, newSegment) {
		t.Fatalf("Different objects (%v vs %v)", segment, newSegment)
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
		segment := NewSegment()
		_ = segment.AddMessages(messages[0:3])

		err := segment.SaveToFile(path.Join(dataDir, segmentFilename(segment.Header)))
		if err != nil {
			t.Fatal(err)
		}

		_ = segment.AddMessages(messages[3:5])

		sFilename = path.Join(dataDir, segmentFilename(segment.Header))
		err = segment.AppendToFile(sFilename)
		if err != nil {
			t.Fatal(err)
		}
	}

	segment, err := SegmentFromFile(sFilename, true)
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
		t.Fatalf("messages are not equal: expected: %v, got: %v", messages, segment.Messages)
	}
}

func TestSegmentedIterator(t *testing.T) {
	topicDir := path.Join(dataDir, "TestSegmentedIterator")
	err := os.MkdirAll(topicDir, 0777)
	if err != nil {
		t.Fatal(err)
	}
	topicStorage := NewSegmentedTopicStorage()
	activeSegment := topicStorage.ActiveSegment()
	// err = activeSegment.SaveToFile(path.Join(topicDir, segmentFilename(activeSegment.Header)))
	if err != nil {
		t.Fatal(err)
	}

	messages := []*message.Message{
		createMessage(1, 0),
		createMessage(2, 500),
		createMessage(3, 100500),
		createMessage(5, 100501),
		createMessage(6, 100600),
		createMessage(10, 100701),
		createMessage(11, 1008000),
	}
	err = activeSegment.AddMessages(messages[0:5])
	if err != nil {
		t.Fatal(err)
	}

	err = activeSegment.AppendToFile(path.Join(topicDir, segmentFilename(activeSegment.Header)))
	if err != nil {
		t.Fatal(err)
	}
	err = activeSegment.UnloadMessages()
	if err != nil {
		t.Fatal(err)
	}

	topicStorage.Segments = append(topicStorage.Segments, NewSegment())
	activeSegment = topicStorage.ActiveSegment()
	err = activeSegment.AddMessages(messages[5:7])
	if err != nil {
		t.Fatal(err)
	}

	iter := NewSegmentedIterator(topicStorage.Segments)
	err = iter.Seek(0)
	if err != nil {
		t.Fatal(err)
	}
	iterMessages := make([]*message.Message, 0)
	for {
		msg, err := iter.Next()
		if err != nil {
			if _, ok := err.(StopIterationError); ok {
				break
			}
			t.Fatal(err)
		}
		iterMessages = append(iterMessages, msg)
	}

	if !reflect.DeepEqual(messages, iterMessages) {
		t.Fatalf("messages are not equal: expected: %v, got: %v", messages, iterMessages)
	}

	iter = NewSegmentedIterator(topicStorage.Segments)
	err = iter.Seek(5)
	if err != nil {
		t.Fatal(err)
	}
	iterMessages = make([]*message.Message, 0)
	for {
		msg, err := iter.Next()
		if err != nil {
			if _, ok := err.(StopIterationError); ok {
				break
			}
			t.Fatal(err)
		}
		iterMessages = append(iterMessages, msg)
	}

	if !reflect.DeepEqual(messages[4:7], iterMessages) {
		t.Fatalf("messages are not equal: expected: %v, got: %v", messages, iterMessages)
	}

	iter = NewSegmentedIterator(topicStorage.Segments)
	err = iter.Seek(6)
	if err != nil {
		t.Fatal(err)
	}
	iterMessages = make([]*message.Message, 0)
	for {
		msg, err := iter.Next()
		if err != nil {
			if _, ok := err.(StopIterationError); ok {
				break
			}
			t.Fatal(err)
		}
		iterMessages = append(iterMessages, msg)
	}

	if !reflect.DeepEqual(messages[5:7], iterMessages) {
		t.Fatalf("messages are not equal: expected: %v, got: %v", messages, iterMessages)
	}

	iter = NewSegmentedIterator(topicStorage.Segments)
	err = iter.Seek(11)
	if err != nil {
		t.Fatal(err)
	}
	iterMessages = make([]*message.Message, 0)
	for {
		msg, err := iter.Next()
		if err != nil {
			if _, ok := err.(StopIterationError); ok {
				break
			}
			t.Fatal(err)
		}
		iterMessages = append(iterMessages, msg)
	}

	if !reflect.DeepEqual([]*message.Message{}, iterMessages) {
		t.Fatalf("messages are not equal: expected: %v, got: %v", messages, iterMessages)
	}

	iter = NewSegmentedIterator(topicStorage.Segments)
	err = iter.Seek(100500)
	if err != nil {
		t.Fatal(err)
	}
	iterMessages = make([]*message.Message, 0)
	for {
		msg, err := iter.Next()
		if err != nil {
			if _, ok := err.(StopIterationError); ok {
				break
			}
			t.Fatal(err)
		}
		iterMessages = append(iterMessages, msg)
	}

	if !reflect.DeepEqual([]*message.Message{}, iterMessages) {
		t.Fatalf("messages are not equal: expected: %v, got: %v", messages, iterMessages)
	}
}
