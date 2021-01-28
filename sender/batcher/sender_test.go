package batcher

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ojarva/remote-cache-server/senders"
	"github.com/ojarva/remote-cache-server/types"
)

type FakeSender struct {
	SentBatches chan types.OutgoingBatch
	Fail        bool
}

func (fs *FakeSender) Init() error {
	return nil
}

func (fs *FakeSender) Send(remoteServerSettings senders.RemoteServerSettings, batch types.OutgoingBatch) error {
	fmt.Printf("Sending %s to %s", batch, remoteServerSettings)
	fs.SentBatches <- batch
	if fs.Fail {
		return errors.New("Failed to send")
	}
	return nil
}
func TestSendBatch(t *testing.T) {
	c := make(chan struct{}, 100)
	inMemoryBatches := types.InMemoryBatches{SlotFreedUp: c}
	inMemoryBatches.SetBatchCount(5)
	sentBatches := make(chan types.OutgoingBatch, 1)
	remoteServerSettings := senders.RemoteServerSettings{Hostname: "testhost", Sender: &FakeSender{SentBatches: sentBatches}}
	inMemoryBatchesAvailable := make(chan struct{})
	go Send(&inMemoryBatches, inMemoryBatchesAvailable, remoteServerSettings)
	inMemoryBatches.Queue(types.OutgoingBatch{BatchID: getBatchID()})
	inMemoryBatchesAvailable <- struct{}{}
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
	c := make(chan struct{}, 100)
	inMemoryBatches := types.InMemoryBatches{SlotFreedUp: c}
	inMemoryBatches.SetBatchCount(5)
	sentBatches := make(chan types.OutgoingBatch, 1)
	remoteServerSettings := senders.RemoteServerSettings{Hostname: "testhost", Sender: &FakeSender{SentBatches: sentBatches, Fail: true}}
	inMemoryBatchesAvailable := make(chan struct{})
	go Send(&inMemoryBatches, inMemoryBatchesAvailable, remoteServerSettings)
	inMemoryBatches.Queue(types.OutgoingBatch{BatchID: getBatchID()})
	inMemoryBatchesAvailable <- struct{}{}
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
	c := make(chan struct{}, 100)
	inMemoryBatches := types.InMemoryBatches{SlotFreedUp: c}
	inMemoryBatches.SetBatchCount(5)
	sentBatches := make(chan types.OutgoingBatch, 1)
	remoteServerSettings := senders.RemoteServerSettings{Hostname: "testhost", Sender: &FakeSender{SentBatches: sentBatches}}
	inMemoryBatchesAvailable := make(chan struct{})
	go Send(&inMemoryBatches, inMemoryBatchesAvailable, remoteServerSettings)
	inMemoryBatchesAvailable <- struct{}{}
}
