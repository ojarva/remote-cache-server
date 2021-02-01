package batcher

import (
	"log"
	"time"

	"github.com/ojarva/remote-cache-server/sender/senders"
	"github.com/ojarva/remote-cache-server/sender/types"
	"github.com/ojarva/remote-cache-server/sender/utils"
)

// Send processes pending batches and tries to send those using sender specified in RemoteServerSettings.
func Send(inMemoryBatches *types.InMemoryBatches, inMemoryBatchesAvailable chan struct{}, remoteServerSettings senders.RemoteServerSettings) {
	var status bool
	backoffUntil := time.Now()
	backoff := utils.BackoffService(remoteServerSettings.MaxBackoffTime)
	ticker := time.NewTicker(10000 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-inMemoryBatchesAvailable:
		case <-ticker.C:
		}
		if backoffUntil.After(time.Now()) {
			// We don't stop the loop with wait in order to avoid blocking inMemoryBatchesAvailable channel
			continue
		}
		batch, err := inMemoryBatches.GetInflight()
		if err != nil {
			if err.Error() == "Nothing to dequeue" {
				continue
			}
			log.Printf("Unable to get a batch from in memory store: %s", err)
			continue
		}
		err = remoteServerSettings.Send(batch)
		if err == nil {
			status = true
		} else {
			log.Printf("Sending batch %s failed with %s", batch.BatchID, err)
			status = false
		}
		backoffDuration := backoff(status)
		backoffUntil = time.Now().Add(backoffDuration)
		err = inMemoryBatches.InflightDone(status)
		if err != nil {
			log.Printf("Unable to dequeue batch: %s", err)
		}
	}
}
