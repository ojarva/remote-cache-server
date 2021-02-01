package runner

import (
	"bufio"
	"log"
	"net"
	"strings"
	"sync"
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
