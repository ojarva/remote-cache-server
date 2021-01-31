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

	"github.com/ojarva/remote-cache-server/receiver/writers"
)

func readConnection(c net.Conn, incomingLineChan chan []byte) {
	reader := bufio.NewReader(c)
	for {
		incoming, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Unable to read from %s: %s", c.RemoteAddr(), err)
			close(incomingLineChan)
			return
		}
		incoming = strings.TrimRight(incoming, "\n")
		if len(incoming) > 0 {
			incomingLineChan <- []byte(incoming)
		}
	}
}

func handleConnection(c net.Conn, outChannel chan []byte) {
	incomingLineChan := make(chan []byte, 10)
	go readConnection(c, incomingLineChan)
	for {
		select {
		case clientMessage := <-incomingLineChan:
			if len(clientMessage) == 0 {
				return
			}
			outChannel <- clientMessage
		}
	}
}

func acceptClients(l *net.TCPListener, newConnectionChannel chan net.Conn) {
	for {
		client, err := l.Accept()
		if err != nil {
			log.Print(err)
			close(newConnectionChannel)
			return
		}
		newConnectionChannel <- client
	}
}

func outputWriter(outChannel chan []byte, writer writers.Writer) {
	for item := range outChannel {
		writer.Write(item)
	}
}

func run(addr *net.TCPAddr, quitChannel chan struct{}, writer writers.Writer) error {
	l, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	outChannel := make(chan []byte, 1000)
	newConnectionChannel := make(chan net.Conn, 100)
	go outputWriter(outChannel, writer)
	go acceptClients(l, newConnectionChannel)
	for {
		select {
		case client := <-newConnectionChannel:
			go handleConnection(client, outChannel)
		case _ = <-quitChannel:
			log.Print("Quit command received, quitting")
			return nil
		}
	}

}
func main() {
	var err error
	portFlag := flag.Int("port", 8080, "Port to listen on")
	ipFlag := flag.String("ip", "127.0.0.1", "IP address to listen on")
	writerFlag := flag.String("writer", "stdout", "Writer to use")
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
	var writer writers.Writer
	switch *writerFlag {
	case "stdout":
		writer = &writers.StdoutWriter{}
	case "file":
		writer = &writers.FileWriter{}
	default:
		log.Fatal("Invalid writer")
	}
	err = writer.Init()
	if err != nil {
		log.Fatalf("Unable to init writer: %s", err)
	}
	defer writer.Close()
	log.Printf("Listening on %d", *portFlag)
	parsedIP := net.ParseIP(*ipFlag)
	if parsedIP == nil {
		log.Fatal("Unable to parse IP")
	}
	addr := net.TCPAddr{IP: parsedIP, Port: *portFlag}
	quitChannel := make(chan struct{})
	osSignalChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalChannel, os.Interrupt)
	go func() {
		for range osSignalChannel {
			log.Print("Received interrupt signal, shutting down")
			close(quitChannel)
		}
	}()
	runErr := run(&addr, quitChannel, writer)
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
		log.Fatalf("Run failed with %s", runErr)
	}
}
