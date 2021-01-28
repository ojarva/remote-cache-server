package backends

import (
	"log"
	"sync"

	"github.com/ojarva/remote-cache-server/types"
)

func FlushInMemoryToDisk(inMemoryBatches *types.InMemoryBatches, quitChannel chan struct{}, fileCacheBackend *FileCacheBackend, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	var batch types.OutgoingBatch
	select {
	case <-quitChannel:
		// Wait until program is quitting
	}
	batchCount := 0
	for {
		batch, err = inMemoryBatches.Dequeue()
		if err != nil {
			log.Printf("Flushing in memory cache to disk finished. %d batches flushed", batchCount)
			return
		}
		err = fileCacheBackend.Save(batch)
		if err != nil {
			log.Printf("Flusing in memory cache to disk failed with %s", err)
		}
		batchCount++
	}
}
