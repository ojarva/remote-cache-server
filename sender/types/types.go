package types

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

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
	SlotFreedUp     chan struct{}
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
	imb.SlotFreedUp <- struct{}{}
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
	imb.SlotFreedUp <- struct{}{}
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
