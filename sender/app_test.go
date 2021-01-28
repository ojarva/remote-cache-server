package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestInMemoryBatchesSize(t *testing.T) {
	imb := InMemoryBatches{}
	imb.SetBatchCount(5)
	var err error
	for i := 0; i < 5; i++ {
		err = imb.Queue(OutgoingBatch{BatchID: fmt.Sprintf("%d", i)})
		if err != nil {
			t.Errorf("Unable to add to queue: %d - %s", i, err)
		}
	}
	err = imb.Queue(OutgoingBatch{BatchID: "failing"})
	if err == nil {
		t.Error("Queue accepted extra item")
	}
	err = imb.SetBatchCount(10)
	if err != nil {
		t.Errorf("Setting batchcount>len(values) failed: %s", err)
	}
	err = imb.SetBatchCount(2)
	if err == nil {
		t.Error("Setting batchcount<len(values) did not fail")
	}
}

func TestInMemoryBatchesDequeue(t *testing.T) {
	imb := InMemoryBatches{}
	imb.SetBatchCount(5)
	var err error
	for i := 0; i < 5; i++ {
		err = imb.Queue(OutgoingBatch{BatchID: fmt.Sprintf("%d", i)})
		if err != nil {
			t.Errorf("Unable to add to queue: %d - %s", i, err)
		}
	}
	batch, err := imb.Dequeue()
	if err != nil {
		t.Errorf("Unable to dequeue from queue: %s", err)
	}
	if batch.BatchID != "0" {
		t.Errorf("Queue returned incorrect item: %s", batch.BatchID)
	}
	for i := 0; i < 4; i++ {
		_, err = imb.Dequeue()
		if err != nil {
			t.Errorf("Unable to dequeue from queue: %s", err)
		}
	}
	_, err = imb.Dequeue()
	if err == nil {
		t.Error("Empty queue did not return an error")
	}
}

func TestInMemoryBatchesInflight(t *testing.T) {
	imb := InMemoryBatches{}
	imb.SetBatchCount(5)
	var err error
	for i := 0; i < 5; i++ {
		err = imb.Queue(OutgoingBatch{BatchID: fmt.Sprintf("%d", i)})
		if err != nil {
			t.Errorf("Unable to add to queue: %d - %s", i, err)
		}
	}
	err = imb.InflightDone(true)
	if err == nil {
		t.Error("InflightDone with empty queue did not fail with an error")
	}
	batch, err := imb.GetInflight()
	if err != nil {
		t.Errorf("GetInflight failed with %s", err)
	}
	if batch.BatchID != "0" {
		t.Errorf("GetInflight did not return the first value: %s", batch.BatchID)
	}
	_, err = imb.GetInflight()
	if err == nil {
		t.Errorf("Duplicate GetInflight did not fail")
	}
	err = imb.InflightDone(false)
	if err != nil {
		t.Errorf("Inflightdone(false) failed: %s", err)
	}
	batch, err = imb.GetInflight()
	if err != nil {
		t.Errorf("GetInflight failed with %s", err)
	}
	if batch.BatchID != "0" {
		t.Errorf("InflightDone(false) dequeued the first value; first value was %s", batch.BatchID)
	}
	err = imb.InflightDone(true)
	if err != nil {
		t.Errorf("Inflightdone(true) failed: %s", err)
	}
	batch, err = imb.GetInflight()
	if err != nil {
		t.Errorf("GetInflight failed with %s", err)
	}
	if batch.BatchID != "1" {
		t.Errorf("InflightDone(true) did not dequeue the first value; first value was %s", batch.BatchID)
	}
}

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

func TestBatchIncomingDataPoints(t *testing.T) {
	incomingChannel := make(chan string, 10)
	outgoingChannel := make(chan OutgoingBatch, 10)
	quitChannel := make(chan struct{})
	maximumBatchWaitTime, _ := time.ParseDuration("1m")
	var wg sync.WaitGroup
	wg.Add(1)
	go batchIncomingDataPoints(incomingChannel, outgoingChannel, 5, quitChannel, maximumBatchWaitTime, &wg)
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

type FakeSender struct {
	SentBatches chan OutgoingBatch
	Fail        bool
}

func (fs *FakeSender) Init() error {
	return nil
}

func (fs *FakeSender) Send(remoteServerSettings RemoteServerSettings, batch OutgoingBatch) error {
	fmt.Printf("Sending %s to %s", batch, remoteServerSettings)
	fs.SentBatches <- batch
	if fs.Fail {
		return errors.New("Failed to send")
	}
	return nil
}

func TestSendBatch(t *testing.T) {
	inMemoryBatches := InMemoryBatches{}
	inMemoryBatches.SetBatchCount(5)
	sentBatches := make(chan OutgoingBatch, 1)
	remoteServerSettings := RemoteServerSettings{Hostname: "testhost", Sender: &FakeSender{SentBatches: sentBatches}}
	inMemoryBatchesAvailable := make(chan bool)
	go sendBatch(&inMemoryBatches, inMemoryBatchesAvailable, remoteServerSettings)
	inMemoryBatches.Queue(OutgoingBatch{BatchID: getBatchID()})
	inMemoryBatchesAvailable <- true
	fmt.Println("Waiting for batch to be sent")
	<-sentBatches
	// Batch was "sent" but there is still a race between marking it as sent and executing the check.
	batchEmpty := false
	for i := 0; i < 10; i++ {
		if inMemoryBatches.Length() == 0 {
			batchEmpty = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !batchEmpty {
		t.Error("inMemoryBatches is not empty")
	}
}

func TestSendBatchFailingSend(t *testing.T) {
	inMemoryBatches := InMemoryBatches{}
	inMemoryBatches.SetBatchCount(5)
	sentBatches := make(chan OutgoingBatch, 1)
	remoteServerSettings := RemoteServerSettings{Hostname: "testhost", Sender: &FakeSender{SentBatches: sentBatches, Fail: true}}
	inMemoryBatchesAvailable := make(chan bool)
	go sendBatch(&inMemoryBatches, inMemoryBatchesAvailable, remoteServerSettings)
	inMemoryBatches.Queue(OutgoingBatch{BatchID: getBatchID()})
	inMemoryBatchesAvailable <- true
	<-sentBatches
	// Batch was "sent" but there is still a race between marking it as sent and executing the check.
	for i := 0; i < 10; i++ {
		if inMemoryBatches.Length() == 0 {
			t.Error("Sending failed but batch was removed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSendBatchNoItems(t *testing.T) {
	inMemoryBatches := InMemoryBatches{}
	inMemoryBatches.SetBatchCount(5)
	sentBatches := make(chan OutgoingBatch, 1)
	remoteServerSettings := RemoteServerSettings{Hostname: "testhost", Sender: &FakeSender{SentBatches: sentBatches}}
	inMemoryBatchesAvailable := make(chan bool)
	go sendBatch(&inMemoryBatches, inMemoryBatchesAvailable, remoteServerSettings)
	inMemoryBatchesAvailable <- true
}

func TestFileBackendFilenames(t *testing.T) {
	fcb := FileCacheBackend{}
	fcb.getCacheFilename(OutgoingBatch{BatchID: "teststring"})
	fcb.getCacheFilename(OutgoingBatch{BatchID: "teststring"})
	filename := fcb.getCacheFilename(OutgoingBatch{BatchID: "teststring"})
	if !strings.HasPrefix(filename, "00000000000000000003") {
		t.Errorf("Invalid filename: %s", filename)

	}
}

func TestFileBackendNoDirectory(t *testing.T) {
	fcb := FileCacheBackend{CacheDir: "non-existing-directory"}
	err := fcb.Init()
	if err == nil {
		t.Error("Init did not return an error")
	}
}

func TestFileBackendInvalidIdentifier(t *testing.T) {
	randomDirectoryID := uuid.New().String()
	os.Mkdir(randomDirectoryID, 0700)
	defer os.RemoveAll(randomDirectoryID)
	os.Create(filepath.Join(randomDirectoryID, "invalidfilename.upload_cache"))
	fcb := FileCacheBackend{CacheDir: randomDirectoryID}
	err := fcb.Init()
	if err == nil {
		t.Error("Init did not return an error")
	}
}

func TestEmptyFileBackend(t *testing.T) {
	randomDirectoryID := uuid.New().String()
	os.Mkdir(randomDirectoryID, 0700)
	defer os.RemoveAll(randomDirectoryID)
	fcb := FileCacheBackend{CacheDir: randomDirectoryID}
	err := fcb.Init()
	if err != nil {
		t.Errorf("Init failed: %s", err)
	}
	filename := fcb.getCacheFilename(OutgoingBatch{BatchID: "testbatch"})
	if !strings.HasPrefix(filename, "00000000000000000001") {
		t.Errorf("Invalid filename: %s", filename)
	}
}

func TestNonEmptyFileBackend(t *testing.T) {
	randomDirectoryID := uuid.New().String()
	os.Mkdir(randomDirectoryID, 0700)
	defer os.RemoveAll(randomDirectoryID)
	os.Create(filepath.Join(randomDirectoryID, "00000000000000000530-2021-01-27T15:19:40.635457+02:00-9ec79b0a-c737-4515-af59-eccd98fc853e.upload_cache"))
	os.Create(filepath.Join(randomDirectoryID, "00000000000000000531-2021-01-27T15:19:40.635457+02:00-9ec79b0a-c737-4515-af59-eccd98fc853e.upload_cache"))
	fcb := FileCacheBackend{CacheDir: randomDirectoryID}
	err := fcb.Init()
	if err != nil {
		t.Errorf("Init failed: %s", err)
	}
	filename := fcb.getCacheFilename(OutgoingBatch{BatchID: "testbatch"})
	if !strings.HasPrefix(filename, "00000000000000000532") {
		t.Errorf("Invalid filename: %s", filename)
	}
}

func TestFileBackendOps(t *testing.T) {
	randomDirectoryID := uuid.New().String()
	os.Mkdir(randomDirectoryID, 0700)
	defer os.RemoveAll(randomDirectoryID)
	fcb := FileCacheBackend{CacheDir: randomDirectoryID}
	var err error
	err = fcb.Init()
	if err != nil {
		t.Errorf("Init failed: %s", err)
	}
	fcb.Save(OutgoingBatch{BatchID: "testid3214", Values: []string{"test1", "test2"}})
	fcb.Save(OutgoingBatch{BatchID: "testid1234", Values: []string{"test3", "test4"}})
	_, err = fcb.GetOldestIdentifier()
	if err != nil {
		t.Errorf("GetOldestIdentifier did not return an error")
	}
	newestFile, err := fcb.GetNewestIdentifier()
	if err != nil {
		t.Errorf("Failed with %s", err)
	}
	oldestFile, err := fcb.GetOldestIdentifier()
	if err != nil {
		t.Errorf("Failed with %s", err)
	}
	if !strings.HasPrefix(newestFile, "00000000000000000002") {
		t.Errorf("Invalid newest filename: %s", newestFile)
	}
	if !strings.HasPrefix(oldestFile, "00000000000000000001") {
		t.Errorf("Invalid oldest filename: %s", err)
	}
	content, err := fcb.GetCachedContent(oldestFile)
	if err != nil {
		t.Errorf("Failed with %s", err)
	}
	if len(content) != 2 || content[0] != "test1" || content[1] != "test2" {
		t.Errorf("Invalid content: %s", content)
	}
	fcb.DeleteCacheItem(oldestFile)
	content, err = fcb.GetCachedContent(oldestFile)
	if err == nil {
		t.Error("GetCachedContent did not fail")
	}
}

func TestBackoffService(t *testing.T) {
	bof := BackoffService(10 * time.Second)
	var waitTime time.Duration
	waitTime = bof(true)
	if waitTime != 0*time.Second {
		t.Errorf("Invalid wait time for successful request: %s", waitTime)
	}
	waitTime = bof(false)
	if waitTime != 2*time.Second {
		t.Errorf("Invalid wait time after failed request: %s", waitTime)
	}
	waitTime = bof(false)
	if waitTime != 4*time.Second {
		t.Errorf("Invalid wait time after failed request: %s", waitTime)
	}
	waitTime = bof(false)
	if waitTime != 8*time.Second {
		t.Errorf("Invalid wait time after failed request: %s", waitTime)
	}
	waitTime = bof(false)
	if waitTime != 10*time.Second {
		t.Errorf("Invalid wait time after failed request: %s", waitTime)
	}
	waitTime = bof(true)
	if waitTime != 0*time.Second {
		t.Errorf("Invalid wait time after successful request: %s", waitTime)
	}
}
