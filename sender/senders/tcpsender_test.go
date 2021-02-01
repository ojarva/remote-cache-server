package senders_test

import (
	"fmt"
	"time"

	"github.com/ojarva/remote-cache-server/sender/senders"
	"github.com/ojarva/remote-cache-server/sender/types"
)

func Example() {
	var err error
	sender := &senders.TCPSender{}
	err = sender.Init()
	if err != nil {
		fmt.Println(err)
		return
	}
	remoteServerSettings := senders.RemoteServerSettings{
		Hostname:       "127.0.0.1",
		Port:           12345,
		MaxBackoffTime: 60 * time.Second,
		Sender:         sender,
	}
	batch := types.OutgoingBatch{
		BatchID: "mytestbatch",
		Values:  []string{"value1", "value2"},
	}
	err = remoteServerSettings.Send(batch)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Output: Unable to open TCP connection: 127.0.0.1:12345: dial tcp 127.0.0.1:12345: connect: connection refused
}
