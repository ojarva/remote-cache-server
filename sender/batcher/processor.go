package batcher

import (
	"log"
	"time"

	"github.com/ojarva/remote-cache-server/backends"
	"github.com/ojarva/remote-cache-server/types"
)

// Processor processes incoming batches, either storing those in InMemoryBatches (if there's room) or to FileCacheBackend. If there's any room in InMemoryBatches, data from files stored by FileCacheBackend are preferred to avoid starvation.
func Processor(batchChannel chan types.OutgoingBatch, inMemoryBatches *types.InMemoryBatches, fileCacheBackend *backends.FileCacheBackend, inMemoryBatchesAvailable chan struct{}, quitChannel chan struct{}) {
	var diskHasItems bool
	var err error
	quitTime := false
	slotFreedUp := func() {
		if quitTime {
			return
		}
		filename, err := fileCacheBackend.GetOldestIdentifier()
		if err == nil {
			diskHasItems = true
			// We have something in file cache -> we need to push that to in memory queue.
			contents, err := fileCacheBackend.GetCachedContent(filename)
			if err == nil {
				batchID := getBatchID()
				err = inMemoryBatches.Queue(types.OutgoingBatch{BatchID: batchID, Values: contents})
				if err == nil {
					// Added to in-memory queue -> remove from disk
					fileCacheBackend.DeleteCacheItem(filename)
					inMemoryBatchesAvailable <- struct{}{}
				}
			}
		} else {
			diskHasItems = false
		}

	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-quitChannel:
			quitTime = true
		case item := <-batchChannel:
			if quitTime {
				err = fileCacheBackend.Save(item)
				if err != nil {
					log.Printf("Batch processor is quitting but unable to write to disk: %s", err)
				}
				continue
			}
			if !diskHasItems {
				err = inMemoryBatches.Queue(item)
				if err == nil {
					inMemoryBatchesAvailable <- struct{}{}
				}
			}
			if err != nil || diskHasItems {
				// No room in local queue/something is waiting on the disk -> push to disk
				err = fileCacheBackend.Save(item)
				if err != nil {
					log.Printf("In memory queue is full but saving to disk failed with %s", err)
				}
			}
		case <-inMemoryBatches.SlotFreedUp:
			slotFreedUp()
		case <-ticker.C:
			slotFreedUp()
		}
	}
}
