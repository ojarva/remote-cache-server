package batcher

import (
	"sync"
	"testing"
)

func TestGetBatchID(t *testing.T) {
	generatedIDs := make(chan string, 100)
	generateIDsMap := make(map[string]struct{})
	var wg sync.WaitGroup
	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		for id := range generatedIDs {
			if _, found := generateIDsMap[id]; found {
				t.Errorf("Duplicate ID %s", id)
			}
			generateIDsMap[id] = struct{}{}
		}
		consumerWg.Done()
	}()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for a := 0; a < 100; a++ {
				generatedIDs <- getBatchID()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	close(generatedIDs)
	consumerWg.Wait()
}

func BenchmarkGetBatchID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		getBatchID()
	}
}
