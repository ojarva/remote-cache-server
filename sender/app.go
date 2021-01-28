package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/ojarva/remote-cache-server/backends"
	"github.com/ojarva/remote-cache-server/batcher"
	"github.com/ojarva/remote-cache-server/senders"
	"github.com/ojarva/remote-cache-server/types"
)

func handleIncomingConnection(client net.Conn, incomingChannel chan string) {
	reader := bufio.NewReader(client)
	for {
		incoming, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Unable to read from %s: %s", client.RemoteAddr(), err)
			return
		}
		incoming = strings.TrimRight(incoming, "\n")
		if len(incoming) > 0 {
			incomingChannel <- incoming
		}
	}
}

func listenIncomingConnections(l *net.TCPListener, newConnection chan net.Conn) {
	for {
		client, err := l.Accept()
		if err != nil {
			log.Print(err)
			return
		}
		newConnection <- client
	}
}

func listenIncoming(port int, incomingChannel chan string, wg *sync.WaitGroup, quitChannel chan struct{}) {
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port}
	l, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		log.Fatalf("Unable to listen on port %d: %s", port, err)
	}
	defer l.Close()
	newConnection := make(chan net.Conn, 10)
	go listenIncomingConnections(l, newConnection)
	log.Printf("Listening on localhost:%d", port)
	for {
		select {
		case client := <-newConnection:
			go handleIncomingConnection(client, incomingChannel)
		case <-quitChannel:
			log.Printf("Quit received; stopped listening")
			wg.Done()
			return
		}
	}
}

func main() {
	protocolFlag := flag.String("protocol", "https", "Protocol")
	remoteServerFlag := flag.String("server", "localhost", "Server address")
	remotePortFlag := flag.Int("port", 8080, "Remote port")
	batchSizeFlag := flag.Int("batch-size", 50, "Number of entries to store in a single batch")
	inMemoryBatchCountFlag := flag.Int("in-memory-batch-count", 50, "Number of batches to keep in memory")
	usernameFlag := flag.String("username", "", "Username for the server. Leave empty for no authentication")
	passwordFlag := flag.String("password", "", "Password for the server. Leave empty for no authentication")
	pathFlag := flag.String("remote-path", "", "Path on the remote server; including any query parameters")
	localDirFlag := flag.String("local-cache-dir", "cache-dir", "Local cache dir for entries that were not sent yet")
	localPortFlag := flag.Int("local-port", 6065, "Port to listen on on localhost")
	maximumBatchWaitTimeFlag := flag.String("maximum-batch-wait-time", "10s", "Maximum time to wait for a batch to fill")
	maxBackoffTimeFlag := flag.String("maximum-backoff-time", "60s", "Maximum backoff time on connection errors")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to file")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	maximumBatchWaitTime, err := time.ParseDuration(*maximumBatchWaitTimeFlag)
	if err != nil {
		log.Fatalf("Invalid maximum-batch-wait-time: %s", err)
	}
	if maximumBatchWaitTime < time.Second {
		log.Fatalf("-maximum-batch-wait-time must be at least 1s")
	}

	quitChannel := make(chan struct{})
	osSignalChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalChannel, os.Interrupt)
	go func() {
		for range osSignalChannel {
			log.Print("Received interrupt signal, shutting down")
			close(quitChannel)
		}
	}()
	log.Printf("Remote server: %s:%d", *remoteServerFlag, *remotePortFlag)
	var wg sync.WaitGroup
	incomingChannel := make(chan string, 100)
	batchChannel := make(chan types.OutgoingBatch, 100)
	inMemoryBatchesAvailable := make(chan struct{}, *inMemoryBatchCountFlag+5)
	inMemorySlotAvailable := make(chan struct{}, 100)
	inMemoryBatches := types.InMemoryBatches{SlotFreedUp: inMemorySlotAvailable}
	var sender senders.Sender
	switch *protocolFlag {
	case "https":
		sender = &senders.HTTPSender{}
	case "http":
		sender = &senders.HTTPSender{}
	case "tcp":
		sender = &senders.TCPSender{}
		if *pathFlag != "" {
			log.Fatal("path is not a valid argument for protocol TCP")
		}
	case "dummy":
		sender = &senders.DummySender{}
	default:
		log.Fatalf("Invalid protocol: %s", *protocolFlag)
	}
	maxBackoffTime, err := time.ParseDuration(*maxBackoffTimeFlag)
	if err != nil {
		log.Fatalf("Invalid max backoff time: %s", err)
	}
	if maxBackoffTime < 0*time.Second {
		log.Fatalf("Maximum backoff time must be >0")
	}
	remoteServerSettings := senders.RemoteServerSettings{
		Protocol:       *protocolFlag,
		Hostname:       *remoteServerFlag,
		Port:           *remotePortFlag,
		Username:       *usernameFlag,
		Password:       *passwordFlag,
		Path:           *pathFlag,
		MaxBackoffTime: maxBackoffTime,
		Sender:         sender,
	}
	fileCacheBackend := &backends.FileCacheBackend{CacheDir: *localDirFlag}
	err = fileCacheBackend.Init()
	if err != nil {
		log.Fatalf("Unable to init file cache backend: %s", err)
	}
	inMemoryBatches.SetBatchCount(*inMemoryBatchCountFlag)
	wg.Add(1)
	go backends.FlushInMemoryToDisk(&inMemoryBatches, quitChannel, fileCacheBackend, &wg)
	wg.Add(1)
	go listenIncoming(*localPortFlag, incomingChannel, &wg, quitChannel)
	wg.Add(1)
	go batcher.BatchIncomingDataPoints(incomingChannel, batchChannel, *batchSizeFlag, quitChannel, maximumBatchWaitTime, &wg)
	go batcher.Send(&inMemoryBatches, inMemoryBatchesAvailable, remoteServerSettings)
	go batcher.Processor(batchChannel, &inMemoryBatches, fileCacheBackend, inMemoryBatchesAvailable, quitChannel)
	wg.Wait()
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		log.Print("Memory profile requested; running GC")
		runtime.GC() // get up-to-date statistics
		log.Print("GC done")
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
