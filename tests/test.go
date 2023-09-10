package main

import (
	"fmt"
	"math/rand"
	"net"
	"os/exec"
	"sync"
	"time"
	"flag"

	util "github.com/achad747/grpc-token-manager/pkg/util"
)

var nodes = []string{
	"localhost:5001", "localhost:5002", "localhost:5003",
	"localhost:5004", "localhost:5005", "localhost:5006",
	"localhost:5007", "localhost:5008", "localhost:5009",
	"localhost:5010",
}

func clientRequest(token util.Token) {
	tokenID := token.ID
	writer := token.Writer
	writerHost, writerPort, _ := net.SplitHostPort(writer)

	switch operation := rand.Intn(10001) % 2; operation {
	case 0: // Read request
		// Select a random reader
		randIndex := rand.Intn(len(token.Readers))
		reader := token.Readers[randIndex]
		readerHost, readerPort, _ := net.SplitHostPort(reader)

		// Execute the command
		cmd := exec.Command("go", "run", "../cmd/tokenclient/main.go", "-method=read", "-id="+tokenID, "-ip="+readerHost, "-port="+readerPort)
		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Printf("Error executing command: %s\n", err)
		}
		fmt.Printf("Output: %s\n", output)

	case 1: // Write request
		low := rand.Uint64() % 3333
		mid := low + 1 + rand.Uint64()%3333
		high := mid + 1 + rand.Uint64()%(10000-mid)

		// Execute the command
		cmd := exec.Command("go", "run", "../cmd/tokenclient/main.go", "-method=write", "-id="+tokenID, "-name=RandomName", fmt.Sprintf("-low=%d", low), fmt.Sprintf("-mid=%d", mid), fmt.Sprintf("-high=%d", high), "-ip="+writerHost, "-port="+writerPort)
		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Printf("Error executing command: %s\n", err)
		}
		fmt.Printf("Output: %s\n", output)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var maxConcurrentThreads int
	flag.IntVar(&maxConcurrentThreads, "threads", 10, "Maximum concurrent threads")
	flag.Parse()

	fmt.Println("Debug: Maximum concurrent threads set to", maxConcurrentThreads)

	var tokens []util.Token

	for i := 1; i <= 1000; i++ {
		writerIndex := (i - 1) % len(nodes)
		writer := nodes[writerIndex]
		readers := []string{
			nodes[(writerIndex+1)%len(nodes)],
			nodes[(writerIndex+2)%len(nodes)],
			nodes[(writerIndex+3)%len(nodes)],
		}

		tokens = append(tokens, util.Token{ID: fmt.Sprintf("Token%d", i%99), Name: "RandomName", Version: 1, Writer: writer, Readers: readers})

	}

	var wg sync.WaitGroup
	sem := make(chan bool, maxConcurrentThreads)

	for _, token := range tokens {
		wg.Add(1)
		sem <- true
		go func(token util.Token) {
			defer wg.Done()
			clientRequest(token)
			<-sem
		}(token)
	}

	wg.Wait()
}
