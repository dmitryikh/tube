package message

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestEmptyMessageSerialization(t *testing.T) {
	message := NewMessage()
	buffer := new(bytes.Buffer)
	err := message.Serialize(buffer)
	if err != nil {
		t.Fatalf("Serialization error: %s", err)
	}

	newMessage := NewMessage()
	err = newMessage.Deserialize(buffer, true)
	if err != nil {
		t.Fatalf("Deserialization error: %s", err)
	}

	fmt.Printf("message: %v, newMessage: %v", message, newMessage)
	if !reflect.DeepEqual(message, newMessage) {
		t.Fatalf("Different objects")
	}
}

func TestMessageSerialization(t *testing.T) {
	message := Message{
		Seq:       10,
		Timestamp: uint64(time.Now().UnixNano()),
		Meta:      make(map[string][]byte),
		Payload:   []byte{1, 2, 3, 4, 5},
	}
	buffer := new(bytes.Buffer)
	err := message.Serialize(buffer)
	if err != nil {
		t.Fatalf("Serialization error: %s", err)
	}

	newMessage := Message{}
	err = newMessage.Deserialize(buffer, true)
	if err != nil {
		t.Fatalf("Deserialization error: %s", err)
	}

	fmt.Printf("message: %v, newMessage: %v", message, newMessage)
	if !reflect.DeepEqual(message, newMessage) {
		t.Fatalf("Different objects")
	}
}
