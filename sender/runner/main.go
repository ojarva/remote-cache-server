package runner

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ojarva/remote-cache-server/sender/backends"
	"github.com/ojarva/remote-cache-server/sender/batcher"
	"github.com/ojarva/remote-cache-server/sender/senders"
	"github.com/ojarva/remote-cache-server/sender/types"
)

type Settings struct {
	LocalPort            int
	LocalDir             string
	InMemoryBatchCount   int
	SenderType           senders.SenderType
	MaxBackoffTime       time.Duration
	BatchSize            int
	MaximumBatchWaitTime time.Duration
	Protocol             string
	RemoteHostname       string
	RemotePort           int
	Username             string
	Password             string
	Path                 string
}

func Run(settings Settings) error {
	var err error
	quitChannel := make(chan struct{})
	osSignalChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalChannel, os.Interrupt)
	go func() {
		for range osSignalChannel {
			log.Print("Received interrupt signal, shutting down")
			close(quitChannel)
		}
	}()
	var wg sync.WaitGroup
	incomingChannel := make(chan string, 100)
	batchChannel := make(chan types.OutgoingBatch, 100)
	inMemoryBatchesAvailable := make(chan struct{}, settings.InMemoryBatchCount+5)
	inMemorySlotAvailable := make(chan struct{}, 100)
	inMemoryBatches := types.InMemoryBatches{SlotFreedUp: inMemorySlotAvailable}
	var sender senders.Sender
	switch settings.SenderType {
	case senders.HTTP:
		sender = &senders.HTTPSender{}
	case senders.TCP:
		sender = &senders.TCPSender{}
	case senders.Dummy:
		sender = &senders.DummySender{}
	}
	remoteServerSettings := senders.RemoteServerSettings{
		Protocol:       settings.Protocol,
		Hostname:       settings.RemoteHostname,
		Port:           settings.RemotePort,
		Username:       settings.Username,
		Password:       settings.Password,
		Path:           settings.Path,
		MaxBackoffTime: settings.MaxBackoffTime,
		Sender:         sender,
	}

	fileCacheBackend := &backends.FileCacheBackend{CacheDir: settings.LocalDir}
	err = fileCacheBackend.Init()
	if err != nil {
		log.Fatalf("Unable to init file cache backend: %s", err)
	}
	inMemoryBatches.SetBatchCount(settings.InMemoryBatchCount)
	wg.Add(1)
	go backends.FlushInMemoryToDisk(&inMemoryBatches, quitChannel, fileCacheBackend, &wg)
	wg.Add(1)
	go listenIncoming(settings.LocalPort, incomingChannel, &wg, quitChannel)
	wg.Add(1)
	go batcher.BatchIncomingDataPoints(incomingChannel, batchChannel, settings.BatchSize, quitChannel, settings.MaximumBatchWaitTime, &wg)
	go batcher.Send(&inMemoryBatches, inMemoryBatchesAvailable, remoteServerSettings)
	go batcher.Processor(batchChannel, &inMemoryBatches, fileCacheBackend, inMemoryBatchesAvailable, quitChannel)
	wg.Wait()
	return nil
}
