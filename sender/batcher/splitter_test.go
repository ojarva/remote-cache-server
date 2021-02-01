package batcher

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ojarva/remote-cache-server/sender/types"
)

func TestBatchIncomingDataPoints(t *testing.T) {
	incomingChannel := make(chan string, 10)
	outgoingChannel := make(chan types.OutgoingBatch, 10)
	quitChannel := make(chan struct{})
	maximumBatchWaitTime, _ := time.ParseDuration("1m")
	var wg sync.WaitGroup
	wg.Add(1)
	go BatchIncomingDataPoints(incomingChannel, outgoingChannel, 5, quitChannel, maximumBatchWaitTime, &wg)
	for i := 0; i < 9; i++ {
		incomingChannel <- fmt.Sprintf("Incoming message %d", i)
	}
	batch := <-outgoingChannel
	if len(batch.Values) != 5 {
		t.Errorf("Invalid batch size: %d", len(batch.Values))
	}
	close(quitChannel)
	batch = <-outgoingChannel
	if len(batch.Values) != 4 {
		t.Errorf("Invalid batch size: %d: %s", len(batch.Values), batch)
	}
}
