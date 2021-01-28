package types

import (
	"fmt"
	"testing"
)

func ExampleInMemoryBatches() {
	inMemoryBatches := InMemoryBatches{SlotFreedUp: make(chan struct{}, 100)}
	inMemoryBatches.SetBatchCount(2)
	inMemoryBatches.Queue(OutgoingBatch{BatchID: "batch1"})
	inMemoryBatches.Queue(OutgoingBatch{BatchID: "batch2"})
	err := inMemoryBatches.Queue(OutgoingBatch{BatchID: "batch3"})
	if err != nil {
		fmt.Println(err)
	}
	item, err := inMemoryBatches.Dequeue()
	if err != nil {
		panic(fmt.Sprintf("Unable to dequeue: %s", err))
	}
	fmt.Println(item.BatchID)
	// Output: Unable to queue, too many items
	// batch1
}

func ExampleInMemoryBatches_InflightDone() {
	inMemoryBatches := InMemoryBatches{SlotFreedUp: make(chan struct{}, 100)}
	inMemoryBatches.SetBatchCount(2)
	inMemoryBatches.Queue(OutgoingBatch{BatchID: "batch1"})
	inMemoryBatches.Queue(OutgoingBatch{BatchID: "batch2"})
	batch, _ := inMemoryBatches.GetInflight()
	fmt.Println(batch.BatchID)
	inMemoryBatches.InflightDone(false)
	batch, _ = inMemoryBatches.GetInflight()
	fmt.Println(batch.BatchID)
	inMemoryBatches.InflightDone(true)
	batch, _ = inMemoryBatches.GetInflight()
	fmt.Println(batch.BatchID)
	// Output: batch1
	// batch1
	// batch2
}

func TestInMemoryBatchesInflight(t *testing.T) {
	c := make(chan struct{}, 100)
	imb := InMemoryBatches{SlotFreedUp: c}
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

func TestInMemoryBatchesSize(t *testing.T) {
	c := make(chan struct{}, 100)
	imb := InMemoryBatches{SlotFreedUp: c}
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
	c := make(chan struct{}, 100)
	imb := InMemoryBatches{SlotFreedUp: c}
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
