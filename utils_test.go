package tube_test

import (
	"testing"
	"time"

	"github.com/dmitryikh/tube"
)

func TestPeriodicThread(t *testing.T) {
	counter := 0
	job := func() { counter += 1 }
	thread := tube.NewPeriodicThread(job, 1)
	time.Sleep(3500 * time.Millisecond)
	thread.Stop()
	thread.Join()
	if counter != 3 {
		t.Fatalf("Counter != 3 (got %d)", counter)
	}
}
