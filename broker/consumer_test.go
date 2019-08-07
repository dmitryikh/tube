package broker

import (
	"reflect"
	"testing"
)

func TestConsumersRegistrySerialization(t *testing.T) {
	registry := ConsumersRegistry{}
	data, err := registry.Serialize()
	if err != nil {
		t.Fatalf("Serialization error: %s", err)
	}

	newRegistry := ConsumersRegistry{}
	err = newRegistry.Deserialize(data)
	if err != nil {
		t.Fatalf("Serialization error: %s", err)
	}

	if !reflect.DeepEqual(registry.Consumers, registry.Consumers) {
		t.Fatalf("Different objects")
	}
}
