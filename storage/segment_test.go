package storage

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

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

	fmt.Printf("segment: %v, newSegment: %v", segment, newSegment)
	if !reflect.DeepEqual(segment, newSegment) {
		t.Fatalf("Different objects")
	}
}
