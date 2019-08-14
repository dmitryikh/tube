package broker

import (
	"bytes"
	"reflect"
	"testing"
)

func TestConsumersRegistrySerialization(t *testing.T) {
	registry := ConsumersRegistry{}
	buffer := new(bytes.Buffer)
	err := registry.Serialize(buffer)
	if err != nil {
		t.Fatalf("Serialization error: %s", err)
	}

	newRegistry := ConsumersRegistry{}
	err = newRegistry.Deserialize(buffer)
	if err != nil {
		t.Fatalf("Serialization error: %s", err)
	}

	if !reflect.DeepEqual(registry.Consumers, registry.Consumers) {
		t.Fatalf("Different objects")
	}
}
