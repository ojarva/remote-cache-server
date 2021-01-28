package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Sender interface {
	Send(RemoteServerSettings, OutgoingBatch) error
	Init() error
}

type DummySender struct{}

func (ds *DummySender) Init() error {
	return nil
}

func (ds *DummySender) Send(remoteServerSettings RemoteServerSettings, batch OutgoingBatch) error {
	return nil
}

type HttpSender struct {
}

func (hs *HttpSender) Init() error {
	return nil
}

func (hs *HttpSender) Send(remoteServerSettings RemoteServerSettings, batch OutgoingBatch) error {
	var url string
	if remoteServerSettings.Protocol == "https" {
		url = remoteServerSettings.GenerateHTTPSURL()
	} else if remoteServerSettings.Protocol == "http" {
		url = remoteServerSettings.GenerateHTTPURL()
	} else {
		panic(fmt.Sprintf("Invalid protocol for httpSender: %s", remoteServerSettings.Protocol))
	}
	batch.Seek(0, io.SeekStart)
	client := &http.Client{}
	req, err := http.NewRequest("POST", url, &batch)
	if err != nil {
		log.Printf("POST request to %s failed with %s", url, err)
		return err
	}
	if remoteServerSettings.Username != "" || remoteServerSettings.Password != "" {
		req.SetBasicAuth(remoteServerSettings.Username, remoteServerSettings.Password)
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("POST request to %s failed with %s", url, err)
		return err
	}
	if resp.StatusCode > 299 {
		log.Printf("POST returned >299: %d. Ignoring.", resp.StatusCode)
	} else {
		log.Printf("Successfully sent %d datapoints to %s", len(batch.Values), url)
	}
	return nil
}

type TcpSender struct {
	conn *net.TCPConn
}

func (ts *TcpSender) Init() error {
	return nil
}

func (ts *TcpSender) Send(remoteServerSettings RemoteServerSettings, batch OutgoingBatch) error {
	if ts.conn == nil {
		// We don't have a connection, try opening one
		hostPort := remoteServerSettings.GenerateTCP()
		addr, err := net.ResolveTCPAddr("tcp", hostPort)
		if err != nil {
			return fmt.Errorf("Unable to resolve %s: %s", hostPort, err)
		}
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			return fmt.Errorf("Unable to open TCP connection: %s: %s", hostPort, err)
		}
		(*ts).conn = conn
	}
	_, err := ts.conn.Write([]byte(strings.Join(batch.Values, "\n")))
	if err != nil {
		ts.conn.Close()
		ts.conn = nil
		return fmt.Errorf("Unable to send data: %s. Closing connection", err)
	}
	return nil
}

// RemoteServerSettings holds details for a remote server
type RemoteServerSettings struct {
	Protocol       string
	Hostname       string
	Port           int
	Path           string
	Username       string
	Password       string
	MaxBackoffTime time.Duration
	Sender         Sender
}

func (rss RemoteServerSettings) Send(batch OutgoingBatch) error {
	return rss.Sender.Send(rss, batch)
}

// String returns a safe string representation without a password
func (rss RemoteServerSettings) String() string {
	return fmt.Sprintf("%s:%d/%s - %s", rss.Hostname, rss.Port, rss.Path, rss.Username)
}

// GenerateHTTPURL returns URL to the endpoint
func (rss RemoteServerSettings) GenerateHTTPURL() string {
	return fmt.Sprintf("http://%s:%d/%s", rss.Hostname, rss.Port, rss.Path)
}

// GenerateHTTPSURL returns URL to the endpoint
func (rss RemoteServerSettings) GenerateHTTPSURL() string {
	return fmt.Sprintf("https://%s:%d/%s", rss.Hostname, rss.Port, rss.Path)
}

func (rss RemoteServerSettings) GenerateTCP() string {
	return fmt.Sprintf("%s:%d", rss.Hostname, rss.Port)
}

// OutgoingBatch represents a single batch of values that is sent at once.
type OutgoingBatch struct {
	BatchID           string
	Values            []string
	readValueIndex    int
	readValuePosition int
}

func (ob OutgoingBatch) String() string {
	return fmt.Sprintf("Batch %s - %s", ob.BatchID, ob.Values)
}

func (ob *OutgoingBatch) Read(p []byte) (int, error) {
	currentLength := 0
	for {
		if len(ob.Values)-1 < ob.readValueIndex {
			// Empty values -> nothing to read
			return currentLength, io.EOF
		}
		charactersIn := copy(p[currentLength:], []byte(ob.Values[ob.readValueIndex][ob.readValuePosition:]+"\n"))
		currentLength += charactersIn
		if ob.readValuePosition+charactersIn < len(ob.Values[ob.readValueIndex])+1 {
			// The whole value did not fit in.
			ob.readValuePosition += charactersIn
			return currentLength, nil
		}
		// That value is done, move on.
		ob.readValueIndex++
		ob.readValuePosition = 0
		if ob.readValueIndex > len(ob.Values) {
			// We reached end of array
			return currentLength, io.EOF
		}
	}
}

// Seek seeks batch pointer to given location. Only whence=start is supported.
func (ob *OutgoingBatch) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, errors.New("Seeking must happen with whence=start")
	}
	if len(ob.Values) == 0 {
		return 0, nil
	}
	if whence == io.SeekStart {
		ob.readValueIndex = 0
		ob.readValuePosition = 0
		if offset == 0 {
			return 0, nil
		}
		var offsetFromStart int64
		for offset > 0 {
			currentObjectLength := int64(len(ob.Values[ob.readValueIndex]) + 1)
			if offset > currentObjectLength {
				offsetFromStart += currentObjectLength
				offset -= currentObjectLength
				if ob.readValueIndex == len(ob.Values)-1 {
					// Woops, we are at the end. Set position the end of the value.
					ob.readValuePosition = len(ob.Values[ob.readValueIndex])
					return offsetFromStart, nil
				}
				ob.readValueIndex++
			} else {
				// We're seeking to the middle of the current value
				offsetFromStart += offset
				ob.readValuePosition = int(offset)
				offset = 0
				return offsetFromStart, nil
			}
		}
	}
	return 0, nil
}

// InMemoryBatches holds in-memory cache of a small number of batches to be sent out. All functions are thread-safe.
type InMemoryBatches struct {
	values          []OutgoingBatch
	writeMutex      sync.Mutex
	inflightSegment bool
	batchCount      int
}

// Queue adds a new item to InMemoryBatches queue. If queue is full, an error is returned.
func (imb *InMemoryBatches) Queue(ob OutgoingBatch) error {
	imb.writeMutex.Lock()
	defer imb.writeMutex.Unlock()
	if len(imb.values) >= imb.batchCount {
		return errors.New("Unable to queue, too many items")
	}
	imb.values = append(imb.values, ob)
	return nil
}

// Dequeue removes and returns the oldest item from InMemoryBatches. If queue is empty or item has been marked as being in flight, an error is returned.
func (imb *InMemoryBatches) Dequeue() (OutgoingBatch, error) {
	imb.writeMutex.Lock()
	defer imb.writeMutex.Unlock()
	if imb.inflightSegment {
		return OutgoingBatch{}, errors.New("Inflight segment, unable to dequeue")
	}
	if len(imb.values) == 0 {
		return OutgoingBatch{}, errors.New("Nothing to dequeue")
	}
	returnValue := imb.values[0]
	imb.values = imb.values[1:]
	return returnValue, nil
}

// GetInflight returns the oldest item without deleting it. The whole queue is marked as being in progress. New values can be queued, but no value can be removed from the queue. An error is returned if queue is empty or if there's already in-flight value that hasn't been marked as being completed.
func (imb *InMemoryBatches) GetInflight() (OutgoingBatch, error) {
	imb.writeMutex.Lock()
	defer imb.writeMutex.Unlock()
	if len(imb.values) == 0 {
		return OutgoingBatch{}, errors.New("Nothing to dequeue")
	}
	if imb.inflightSegment {
		return OutgoingBatch{}, errors.New("Inflight segment already activated")
	}
	returnValue := imb.values[0]
	imb.inflightSegment = true
	return returnValue, nil
}

// InflightDone marks the oldest item as being available again (if success is false) or removes the oldest item (if success is true). An error is returned if the oldest item was not marked as being in-flight with GetInflight.
func (imb *InMemoryBatches) InflightDone(success bool) error {
	imb.writeMutex.Lock()
	defer imb.writeMutex.Unlock()
	if !imb.inflightSegment {
		return errors.New("No inflight segment, unable to dequeue")
	}
	imb.inflightSegment = false
	if !success {
		return nil
	}
	if len(imb.values) == 0 {
		// Classic "this should never happen" error message.
		// If the state has not been messed up with reflection or if there isn't a bug in GetInflight/InflightDone/Dequeue, we should never get to here.
		panic("InflightDone trying to dequeue from empty queue. This should never happen")
	}
	imb.values = imb.values[1:]
	return nil
}

// SetBatchCount sets the maximum number of batches that can be kept in-memory at the same time.
func (imb *InMemoryBatches) SetBatchCount(count int) error {
	imb.writeMutex.Lock()
	defer imb.writeMutex.Unlock()
	if len(imb.values) > count {
		return errors.New("Too many entries; unable to shrink")
	}
	imb.batchCount = count
	return nil
}

// Length returns current number of batches hold in-memory, including any that has been marked as being in-flight.
func (imb *InMemoryBatches) Length() int {
	imb.writeMutex.Lock()
	defer imb.writeMutex.Unlock()
	return len(imb.values)
}

// CacheBackend is used to persist overflow when sending is either lagging behind or failing (for example, because of any connectivity issues).
type CacheBackend interface {
	Init() error
	GetOldestIdentifier() (string, error)
	GetNewestIdentifier() (string, error)
	GetCachedContent(identifier string) ([]string, error)
	DeleteCacheItem(identifier string) error
	Save(ob OutgoingBatch) error
}

// FileCacheBackend is a basic backend for storing overflow data in the filesystem, inside a single folder.
type FileCacheBackend struct {
	CacheDir        string
	lastIdentifier  uint64
	identifierMutex sync.Mutex
}

// Init initializes FileCacheBackend, checking preconditions (such as existence of the cache directory) and loads current identifier, if available.
func (fcb *FileCacheBackend) Init() error {
	info, err := os.Stat(fcb.CacheDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", fcb.CacheDir)
	}
	fullIdentifier, err := fcb.GetNewestIdentifier()
	if err == nil {
		identifierString := strings.Split(fullIdentifier, "-")[0]
		parsedIdentifier, err := strconv.ParseUint(identifierString, 10, 64)
		if err != nil {
			return fmt.Errorf("Invalid identifier from filename: %s", err)
		}
		(*fcb).lastIdentifier = parsedIdentifier
	}
	return nil
}

func (fcb *FileCacheBackend) getFilename(identifier string) string {
	return filepath.Join(fcb.CacheDir, identifier)
}

func (fcb *FileCacheBackend) getCacheFilename(ob OutgoingBatch) string {
	identifier := atomic.AddUint64(&fcb.lastIdentifier, 1)
	return fmt.Sprintf("%020d-%s-%s.upload_cache", identifier, time.Now().Format(time.RFC3339Nano), ob.BatchID)
}

func (fcb *FileCacheBackend) getAllFiles() []string {
	files, err := filepath.Glob(filepath.Join(fcb.CacheDir, "*.upload_cache"))
	if err != nil {
		panic(fmt.Sprintf("Unable to list files: %s", err))
	}
	sort.Strings(files)
	return files
}

// GetOldestIdentifier returns the oldest identifier. An error is returned if no files are available.
func (fcb *FileCacheBackend) GetOldestIdentifier() (string, error) {
	files := fcb.getAllFiles()
	if len(files) > 0 {
		return filepath.Base(files[0]), nil
	}
	return "", fmt.Errorf("Unable to get the oldest identifier: no files available")
}

// GetNewestIdentifier returns the newest identifier. An error is returned if no files are available.
func (fcb *FileCacheBackend) GetNewestIdentifier() (string, error) {
	files := fcb.getAllFiles()
	if len(files) > 0 {
		return filepath.Base(files[len(files)-1]), nil
	}
	return "", fmt.Errorf("Unable to get the newest identifier: no files available")
}

// GetCachedContent gets cached file contents. File is not removed from the disk.
func (fcb *FileCacheBackend) GetCachedContent(identifier string) ([]string, error) {
	filename := fcb.getFilename((identifier))
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Unable to read cached file: %s - %s", identifier, err)
		return nil, err
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// DeleteCacheItem removes item from cache
func (fcb *FileCacheBackend) DeleteCacheItem(identifier string) error {
	filename := fcb.getFilename((identifier))
	log.Printf("Deleting %s", filename)
	return os.Remove(filename)
}

// Save saves OutgoingBatch to persistent storage
func (fcb *FileCacheBackend) Save(ob OutgoingBatch) error {
	filename := fcb.getFilename(fcb.getCacheFilename(ob))
	log.Printf("Saving to %s", filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("Unable to save to %s - %s", filename, err)
	}
	defer file.Close()
	for _, item := range ob.Values {
		file.WriteString(item + "\n")
	}
	return nil
}
func getBatchID() string {
	return uuid.New().String()
}

func batchIncomingDataPoints(incomingChannel chan string, outgoingChannel chan OutgoingBatch, batchSize int, quitChannel chan struct{}, maximumBatchWaitTime time.Duration, wg *sync.WaitGroup) {
	var localItems []string
	var item string

	createBatch := func(localItems *[]string) {
		if len(*localItems) > 0 {
			batchID := getBatchID()
			itemCount := len(*localItems)
			log.Printf("Creating a new batch %s with %d items", batchID, itemCount)
			outgoingSlice := make([]string, itemCount)
			copy(outgoingSlice, *localItems)
			outgoingChannel <- OutgoingBatch{BatchID: batchID, Values: outgoingSlice}
			*localItems = make([]string, 0, batchSize)
		}
	}
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
		case <-time.Tick(maximumBatchWaitTime):
			createBatch(&localItems)
		}
	}
}

func flushInMemoryToDisk(inMemoryBatches *InMemoryBatches, quitChannel chan struct{}, fileCacheBackend *FileCacheBackend, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	var batch OutgoingBatch
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

// BackoffService provides a simple exponential backoff.
func BackoffService(maxWaitTime time.Duration) func(success bool) time.Duration {
	var failureCount int
	return func(success bool) time.Duration {
		if success {
			// We succeeded on the last iteration; it's ok to retry immediately.
			failureCount = 0
			return 0 * time.Second
		}
		failureCount++
		waitTime, _ := time.ParseDuration(fmt.Sprintf("%.0fs", math.Pow(2, float64(failureCount))))
		if waitTime > maxWaitTime {
			return maxWaitTime
		}
		return waitTime
	}
}

func sendBatch(inMemoryBatches *InMemoryBatches, inMemoryBatchesAvailable chan struct{}, remoteServerSettings RemoteServerSettings) {
	var status bool
	backoffUntil := time.Now()
	backoff := BackoffService(remoteServerSettings.MaxBackoffTime)
	for {
		select {
		case <-inMemoryBatchesAvailable:
		case <-time.Tick(100 * time.Millisecond):
		}
		if backoffUntil.After(time.Now()) {
			// We don't stop the loop with wait in order to avoid blocking inMemoryBatchesAvailable channel
			continue
		}
		batch, err := inMemoryBatches.GetInflight()
		if err != nil {
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

func batchProcessor(batchChannel chan OutgoingBatch, inMemoryBatches *InMemoryBatches, fileCacheBackend *FileCacheBackend, inMemoryBatchesAvailable chan struct{}, quitChannel chan struct{}) {
	var diskHasItems bool
	var err error
	var lastCachedFileCheck time.Time
	var lastIterationFoundFile bool
	quitTime := false
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
		case <-time.Tick(10 * time.Millisecond):
			if quitTime {
				continue
			}
			timeSinceLastCheck := time.Now().Sub(lastCachedFileCheck)
			if timeSinceLastCheck < 1*time.Second && !lastIterationFoundFile {
				continue
			}
			lastCachedFileCheck = time.Now()
			lastIterationFoundFile = false
			filename, err := fileCacheBackend.GetOldestIdentifier()
			if err == nil {
				diskHasItems = true
				// We have something in file cache -> we need to push that to in memory queue.
				contents, err := fileCacheBackend.GetCachedContent(filename)
				if err == nil {
					batchID := getBatchID()
					err = inMemoryBatches.Queue(OutgoingBatch{BatchID: batchID, Values: contents})
					if err == nil {
						// Added to in-memory queue -> remove from disk
						fileCacheBackend.DeleteCacheItem(filename)
						inMemoryBatchesAvailable <- struct{}{}
						lastIterationFoundFile = true
					}
				}
			} else {
				diskHasItems = false
			}
		}
	}
}

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
	batchChannel := make(chan OutgoingBatch, 100)
	inMemoryBatchesAvailable := make(chan struct{}, *inMemoryBatchCountFlag+5)
	var inMemoryBatches InMemoryBatches
	var sender Sender
	switch *protocolFlag {
	case "https":
		sender = &HttpSender{}
	case "http":
		sender = &HttpSender{}
	case "tcp":
		sender = &TcpSender{}
		if *pathFlag != "" {
			log.Fatal("path is not a valid argument for protocol TCP")
		}
	case "dummy":
		sender = &DummySender{}
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
	remoteServerSettings := RemoteServerSettings{
		Protocol:       *protocolFlag,
		Hostname:       *remoteServerFlag,
		Port:           *remotePortFlag,
		Username:       *usernameFlag,
		Password:       *passwordFlag,
		Path:           *pathFlag,
		MaxBackoffTime: maxBackoffTime,
		Sender:         sender,
	}
	fileCacheBackend := &FileCacheBackend{CacheDir: *localDirFlag}
	err = fileCacheBackend.Init()
	if err != nil {
		log.Fatalf("Unable to init file cache backend: %s", err)
	}
	inMemoryBatches.SetBatchCount(*inMemoryBatchCountFlag)
	wg.Add(1)
	go flushInMemoryToDisk(&inMemoryBatches, quitChannel, fileCacheBackend, &wg)
	wg.Add(1)
	go listenIncoming(*localPortFlag, incomingChannel, &wg, quitChannel)
	wg.Add(1)
	go batchIncomingDataPoints(incomingChannel, batchChannel, *batchSizeFlag, quitChannel, maximumBatchWaitTime, &wg)
	go sendBatch(&inMemoryBatches, inMemoryBatchesAvailable, remoteServerSettings)
	go batchProcessor(batchChannel, &inMemoryBatches, fileCacheBackend, inMemoryBatchesAvailable, quitChannel)
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
