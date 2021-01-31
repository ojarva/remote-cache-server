package senders

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/ojarva/remote-cache-server/types"
)

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

// Send sends the data to remote server
func (rss RemoteServerSettings) Send(batch types.OutgoingBatch) error {
	return rss.Sender.Send(rss, batch)
}

func (rss RemoteServerSettings) cleanPath() string {
	return strings.TrimLeft(rss.Path, "/")
}

// String returns a safe string representation without a password
func (rss RemoteServerSettings) String() string {
	path := rss.cleanPath()
	return fmt.Sprintf("%s:%d/%s - %s", rss.Hostname, rss.Port, path, rss.Username)
}

// GenerateHTTPURL returns URL to the endpoint
func (rss RemoteServerSettings) GenerateHTTPURL() string {
	path := rss.cleanPath()
	return fmt.Sprintf("http://%s:%d/%s", rss.Hostname, rss.Port, path)
}

// GenerateHTTPSURL returns URL to the endpoint
func (rss RemoteServerSettings) GenerateHTTPSURL() string {
	path := rss.cleanPath()
	return fmt.Sprintf("https://%s:%d/%s", rss.Hostname, rss.Port, path)
}

// GenerateTCP generates connection string for TCP
func (rss RemoteServerSettings) GenerateTCP() string {
	return fmt.Sprintf("%s:%d", rss.Hostname, rss.Port)
}

// Sender defines an interface for a component used to send data forward
type Sender interface {
	Send(RemoteServerSettings, types.OutgoingBatch) error
	Init() error
}

// DummySender discards all incoming data without a delay and never fails.
type DummySender struct{}

// Init initializes the sender
func (ds *DummySender) Init() error {
	return nil
}

// Send sends the data to remote server
func (ds *DummySender) Send(remoteServerSettings RemoteServerSettings, batch types.OutgoingBatch) error {
	return nil
}

// HTTPSender sends data over HTTP or HTTPS, depending on the protocol
type HTTPSender struct {
}

// Init initializes the sender
func (hs *HTTPSender) Init() error {
	return nil
}

// Send sends the data to remote server
func (hs *HTTPSender) Send(remoteServerSettings RemoteServerSettings, batch types.OutgoingBatch) error {
	var url string
	if remoteServerSettings.Protocol == "https" {
		url = remoteServerSettings.GenerateHTTPSURL()
	} else if remoteServerSettings.Protocol == "http" {
		url = remoteServerSettings.GenerateHTTPURL()
	} else {
		panic(fmt.Sprintf("Invalid protocol for HTTPSender: %s", remoteServerSettings.Protocol))
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

// TCPSender sends the data to a TCP socket
type TCPSender struct {
	conn *net.TCPConn
}

// Init initializes the sender
func (ts *TCPSender) Init() error {
	return nil
}

// Send sends the data to remote server
func (ts *TCPSender) Send(remoteServerSettings RemoteServerSettings, batch types.OutgoingBatch) error {
	var err error
	if ts.conn == nil {
		// We don't have a connection, try opening one
		hostPort := remoteServerSettings.GenerateTCP()
		dialer := net.Dialer{Timeout: 5 * time.Second}
		conn, err := dialer.Dial("tcp", hostPort)
		if err != nil {
			return fmt.Errorf("Unable to open TCP connection: %s: %s", hostPort, err)
		}
		(*ts).conn = conn.(*net.TCPConn)
	}
	ts.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = ts.conn.Write([]byte(strings.Join(batch.Values, "\n")))
	if err == nil {
		_, err = ts.conn.Write([]byte("\n"))
	}
	if err != nil {
		ts.conn.Close()
		ts.conn = nil
		return fmt.Errorf("Unable to send data: %s. Closing connection", err)
	}
	return nil
}
