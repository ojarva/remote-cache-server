package main

import (
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/ojarva/remote-cache-server/sender/runner"
	"github.com/ojarva/remote-cache-server/sender/senders"
)

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

	log.Printf("Remote server: %s:%d", *remoteServerFlag, *remotePortFlag)
	var sender senders.SenderType
	switch *protocolFlag {
	case "https":
		sender = senders.HTTP
	case "http":
		sender = senders.HTTP
	case "tcp":
		sender = senders.TCP
		if *pathFlag != "" {
			log.Fatal("path is not a valid argument for protocol TCP")
		}
	case "dummy":
		sender = senders.Dummy
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

	settings := runner.Settings{
		LocalPort:            *localPortFlag,
		LocalDir:             *localDirFlag,
		InMemoryBatchCount:   *inMemoryBatchCountFlag,
		SenderType:           sender,
		MaxBackoffTime:       maxBackoffTime,
		BatchSize:            *batchSizeFlag,
		MaximumBatchWaitTime: maximumBatchWaitTime,
		Protocol:             *protocolFlag,
		RemoteHostname:       *remoteServerFlag,
		RemotePort:           *remotePortFlag,
		Username:             *usernameFlag,
		Password:             *passwordFlag,
		Path:                 *pathFlag,
	}

	runErr := runner.Run(settings)
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
	if runErr != nil {
		log.Fatalf("Run failed with %s", err)
	}
}
