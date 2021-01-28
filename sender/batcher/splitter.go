package batcher

import (
	"log"
	"sync"
	"time"

	"github.com/ojarva/remote-cache-server/types"
)

func BatchIncomingDataPoints(incomingChannel chan string, outgoingChannel chan types.OutgoingBatch, batchSize int, quitChannel chan struct{}, maximumBatchWaitTime time.Duration, wg *sync.WaitGroup) {
	var localItems []string
	var item string

	createBatch := func(localItems *[]string) {
		if len(*localItems) > 0 {
			batchID := getBatchID()
			itemCount := len(*localItems)
			log.Printf("Creating a new batch %s with %d items", batchID, itemCount)
			outgoingSlice := make([]string, itemCount)
			copy(outgoingSlice, *localItems)
			outgoingChannel <- types.OutgoingBatch{BatchID: batchID, Values: outgoingSlice}
			*localItems = make([]string, 0, batchSize)
		}
	}
	ticker := time.NewTicker(maximumBatchWaitTime)
	defer ticker.Stop()
	for {
		select {
		case item = <-incomingChannel:
			if len(localItems) >= batchSize {
				createBatch(&localItems)
			}
			localItems = append(localItems, item)
		case <-quitChannel:
			createBatch(&localItems)
			log.Print("Quit received; stopped batching incoming datapoints")
			wg.Done()
			return
		case <-ticker.C:
			createBatch(&localItems)
		}
	}
}
