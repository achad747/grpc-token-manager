package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"sync"

	token "github.com/achad747/grpc-token-manager/pkg/api"
	"gopkg.in/yaml.v2"

	"google.golang.org/grpc"
)

type Token struct {
	ID      string   `yaml:"id"`
	Name    string   `yaml:"name"`
	Domain  Domain   `yaml:"domain"`
	State   State    `yaml:"state"`
	Version int64    `yaml:"version"`
	Writer  string   `yaml:"writer"`
	Readers []string `yaml:"readers"`
}

type Domain struct {
	Low  uint64 `yaml:"low"`
	Mid  uint64 `yaml:"mid"`
	High uint64 `yaml:"high"`
}

type State struct {
	Partial uint64 `yaml:"partial"`
	Final   uint64 `yaml:"final"`
}

type TokensList struct {
	Tokens []Token `yaml:"tokens"`
}

type ConnectionPool struct {
	mu                   sync.Mutex
	connections          map[string]chan *grpc.ClientConn
	connectionCounts     map[string]int
	cond                 *sync.Cond
	maxConnectionsPerAddr int
}

type TokenData struct {
    token *Token
}

type Server struct {
    token.UnimplementedTokenServiceServer
	tokens map[string]*Token
	mu     sync.RWMutex
    connPool   *ConnectionPool
}

func (s *Server) Create(ctx context.Context, req *token.TokenRequest) (*token.TokenResponse, error) {
    if req.GetId() == "" {
		return nil, fmt.Errorf("ID must not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tokens[req.GetId()]; exists {
		return nil, fmt.Errorf("Token with ID %s already exists", req.GetId())
	}

	s.tokens[req.GetId()] = &Token{
		ID: req.GetId(),
	}

    return &token.TokenResponse{Status: "success"}, nil
}

func (s *Server) Drop(ctx context.Context, req *token.TokenRequest) (*token.TokenResponse, error) {
    if req.GetId() == "" {
		return nil, fmt.Errorf("ID must not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tokens[req.GetId()]; !ok {
		return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
	}

    delete(s.tokens, req.GetId())
    
    return &token.TokenResponse{Status: "success"}, nil
}

func (s *Server) Write(ctx context.Context, req *token.WriteRequest) (*token.TokenResponse, error) {
	if req.GetId() == "" || req.GetName() == "" {
		return nil, fmt.Errorf("ID and Name must not be empty")
	}

	if req.GetLow() >= req.GetMid() || req.GetMid() >= req.GetHigh() {
		return nil, fmt.Errorf("Domain bounds are invalid. It should satisfy: Low <= Mid < High")
	}

	s.mu.Lock()
	tokenData, ok := s.tokens[req.GetId()]
	s.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
	}

    if req.GetName() != "" && req.GetLow() != 0 {
        tokenData.Name = req.GetName()
        tokenData.Domain.Low = req.GetLow()
        tokenData.Domain.Mid = req.GetMid()
        tokenData.Domain.High = req.GetHigh()
    }

	/*Single thread hash
    minValue := uint64(1<<63 - 1)
	for x := tokenData.Domain.Low; x < tokenData.Domain.Mid; x++ {
		currentHash := Hash(tokenData.Name, x)
		if currentHash < minValue {
			minValue = currentHash
		}
	}*/

    _, partialVal := FindMinHashWithNonce(tokenData, tokenData.Domain.Low, tokenData.Domain.Mid)
    _, finalValue := FindMinHashWithNonce(tokenData, tokenData.Domain.Mid, tokenData.Domain.High)

    tokenData.State.Partial = partialVal
	tokenData.State.Final = Min(finalValue, tokenData.State.Partial)
    tokenData.Version++

    s.mu.Lock()
	s.tokens[req.GetId()] = tokenData
	s.mu.Unlock()

    var wg sync.WaitGroup
    errors := make(chan error, len(tokenData.Readers))

    for _, reader := range tokenData.Readers {
        wg.Add(1)
        go func(readerAddr string) {
            defer wg.Done()
            err := s.sendWriteToNode(readerAddr, tokenData)
            if err != nil {
                fmt.Errorf("failed to replicate to reader: %v", err)
                errors <- err
            }
        }(reader)
    }

    // Wait for all goroutines to complete.
    wg.Wait()

    close(errors)

    return &token.TokenResponse{FinalValue:tokenData.State.Final, 
        PartialValue: tokenData.State.Partial, Status: "success"}, nil
}

func (s *Server) Read(ctx context.Context, req *token.TokenRequest) (*token.TokenResponse, error) {
	if req.GetId() == "" {
        return nil, fmt.Errorf("ID must not be empty")
    }

    s.mu.RLock()
    tokenData, ok := s.tokens[req.GetId()]
    s.mu.RUnlock()

    if !ok {
        return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
    }

    // Step 1: Read local value.
    localVersion := tokenData.Version
    localValue := tokenData.State.Final

    // Send read requests to all reader replicas, including itself.
    responses := make(chan *token.WriteReplicaRequest, len(tokenData.Readers))
    errorsCh := make(chan error, len(tokenData.Readers))

    var wg sync.WaitGroup
    for _, reader := range tokenData.Readers {
        wg.Add(1)
        go func(readerAddr string, localVersion int64) {
            defer wg.Done()
            resp, err := s.sendReadToNode(readerAddr, req.GetId())
            if err != nil {
                errorsCh <- err
                fmt.Println("Error in read replication:", err)
                return
            }
            responses <- resp
        }(reader, localVersion)
    }
    wg.Wait()
    close(responses)
    close(errorsCh)

    // Process the responses to get the maximum version and its corresponding value.
    maxVersion := localVersion
    maxValue := localValue
    for resp := range responses {
        if resp.Version > maxVersion {
            maxVersion = resp.Version
            maxValue = resp.Final
        }
    }
    
    // Step 3: Impose the maximum value to all readers.
    imposeRequests := make(chan error, len(tokenData.Readers))
    for _, reader := range tokenData.Readers {
        wg.Add(1)
        go func(readerAddr string) {
            defer wg.Done()
            err := s.sendImposeToNode(readerAddr, req.GetId(), maxVersion, maxValue)
            if err != nil {
                imposeRequests <- err
                return
            }
        }(reader)
    }
    wg.Wait()
    close(imposeRequests)

    for err := range imposeRequests {
        log.Println("Error in impose step:", err)
    }

    // Update the local value and version if necessary.
    s.mu.Lock()
    if maxVersion > tokenData.Version {
        tokenData.Version = maxVersion
        tokenData.State.Final = maxValue
        s.tokens[req.GetId()] = tokenData
    }
    s.mu.Unlock()

    return &token.TokenResponse{FinalValue:tokenData.State.Final, 
        PartialValue: tokenData.State.Partial, Status: "success"}, nil
}

func (s *Server) sendReadToNode(nodeAddress string, tokenID string) (*token.WriteReplicaRequest, error) {
    conn, err := s.connPool.GetConn(nodeAddress)
    if err != nil {
        return nil, err
    }
    defer s.connPool.ReturnConn(nodeAddress, conn)

    client := token.NewTokenServiceClient(conn)
    response, err := client.ReadFromReplica(context.Background(), &token.TokenRequest{Id: tokenID}) // Use ReplicaRead here
    if err != nil {
        return nil, err
    }

    // Convert the response to our Token struct.
    returnToken := &token.WriteReplicaRequest{
        Id: response.Id,
        Name: response.Name,
        Low: response.Low,
        Mid: response.Mid,
        High: response.High,
        Partial: response.Partial,
        Final: response.Final,
        Version: response.Version,
    }
    return returnToken, nil
}

func (s *Server) ReadFromReplica(ctx context.Context, req *token.TokenRequest) (*token.WriteReplicaRequest, error) {
    s.mu.RLock()
    tokenData, ok := s.tokens[req.GetId()]
    s.mu.RUnlock()

    if !ok {
        return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
    }

    // Simply return the local token's state without further reading from replicas.
    return &token.WriteReplicaRequest{
        Id: tokenData.ID,
        Name: tokenData.Name,
        Low: tokenData.Domain.Low,
        Mid: tokenData.Domain.Mid,
        High: tokenData.Domain.High,
        Partial: tokenData.State.Partial,
        Final: tokenData.State.Final,
        Version: tokenData.Version,
    }, nil
}

func (s *Server) sendImposeToNode(nodeAddress string, tokenID string, version int64, value uint64) error {
    tmpToken := &Token{
        ID:      tokenID,
        Version: version,
        State:   State{Final: value},
    }

    return s.sendWriteToNode(nodeAddress, tmpToken)
}

func Hash(name string, nonce uint64) uint64 {
    hasher := sha256.New()
    hasher.Write([]byte(fmt.Sprintf("%s %d", name, nonce)))
    return binary.BigEndian.Uint64(hasher.Sum(nil))
}

func Min(a, b uint64) uint64 {
    if a < b {
        return a
    }
    return b
}

func FindMinHashWithNonce(tokenData *Token, start, end uint64) (uint64, uint64) {
    numGoroutines := 3
    chunkSize := (end - start) / uint64(numGoroutines)

    results := make(chan struct {
        hash  uint64
        nonce uint64
    }, numGoroutines)

    for i := 0; i < numGoroutines; i++ {
        currentStart := start + uint64(i)*chunkSize
        currentEnd := currentStart + chunkSize
        if i == numGoroutines-1 {
            currentEnd = end
        }

        go func(s, e uint64) {
            localMin := uint64(1<<63 - 1)
            localNonce := s
            for x := s; x < e; x++ {
                currentHash := Hash(tokenData.Name, x)
                if currentHash < localMin {
                    localMin = currentHash
                    localNonce = x
                }
            }
            results <- struct {
                hash  uint64
                nonce uint64
            }{localMin, localNonce}
        }(currentStart, currentEnd)
    }

    minValue := uint64(1<<63 - 1)
    minNonce := start

    for i := 0; i < numGoroutines; i++ {
        result := <-results
        if result.hash < minValue {
            minValue = result.hash
            minNonce = result.nonce
        }
    }

    close(results)
    return minValue, minNonce
}

func (s *Server) sendWriteToNode(nodeAddress string, t *Token) error {
    conn, err := s.connPool.GetConn(nodeAddress)
    if err != nil {
        return err
    }
    defer s.connPool.ReturnConn(nodeAddress, conn)

    client := token.NewTokenServiceClient(conn)

    _, err = client.WriteToReplica(context.Background(), &token.WriteReplicaRequest{
        Id:      t.ID,
        Name:    t.Name,
        Low:     t.Domain.Low,
        Mid:     t.Domain.Mid,
        High:    t.Domain.High,
        Partial: t.State.Partial,
        Final:   t.State.Final,
        Version: t.Version,
    })

    return err
}

func (s *Server) WriteToReplica(ctx context.Context, req *token.WriteReplicaRequest) (*token.TokenResponse, error) {
    // Similar to the Write function, but without triggering further replication.
    // Just update the local state based on the replicated data.

    // Retrieve the token data
    s.mu.Lock()
    tokenData, exists := s.tokens[req.GetId()]
    s.mu.Unlock()

    if !exists {
        return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
    }

    // Update the token's data based on the replicated data
    tokenData.Name = req.GetName()
    tokenData.Domain.Low = req.GetLow()
    tokenData.Domain.Mid = req.GetMid()
    tokenData.Domain.High = req.GetHigh()
    tokenData.State.Partial = req.GetPartial()
    tokenData.State.Final = req.GetFinal()
    tokenData.Version = req.GetVersion()

    s.mu.Lock()
    s.tokens[req.GetId()] = tokenData
    s.mu.Unlock()

    return &token.TokenResponse{Status: "success"}, nil
}

func NewConnectionPool(maxPerAddr int) *ConnectionPool {
	return &ConnectionPool{
		connections:          make(map[string]chan *grpc.ClientConn),
		connectionCounts:     make(map[string]int),
		cond:                 sync.NewCond(&sync.Mutex{}),
		maxConnectionsPerAddr: maxPerAddr,
	}
}

func (cp *ConnectionPool) GetConn(address string) (*grpc.ClientConn, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.connections[address] == nil {
		cp.connections[address] = make(chan *grpc.ClientConn, cp.maxConnectionsPerAddr)
	}

	if len(cp.connections[address]) > 0 {
		return <-cp.connections[address], nil
	}

	// If we reach here, there were no available connections in the pool for the given address.
	// Check if we can create a new connection.
	if cp.connectionCounts[address] < cp.maxConnectionsPerAddr {
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		cp.connectionCounts[address]++
		return conn, nil
	}

	// If maximum connections are reached and none are available, wait.
	cp.cond.L.Lock()
	for len(cp.connections[address]) == 0 && cp.connectionCounts[address] >= cp.maxConnectionsPerAddr {
		cp.cond.Wait()
	}
	cp.cond.L.Unlock()

	return <-cp.connections[address], nil
}

func (cp *ConnectionPool) ReturnConn(address string, conn *grpc.ClientConn) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.connections[address] == nil {
		cp.connections[address] = make(chan *grpc.ClientConn, cp.maxConnectionsPerAddr)
	}

	select {
	case cp.connections[address] <- conn:
		cp.cond.Broadcast()
	default:
		// Optional: handle the case where the connection channel is full. This shouldn't happen in the current design.
	}
}

func (cp *ConnectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, conns := range cp.connections {
		close(conns)
		for conn := range conns {
			conn.Close()
		}
	}
}

func main() {
    lis, err := net.Listen("tcp", ":50051")
    fmt.Printf("Starting server on port :50051")
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }
    s := grpc.NewServer()

    tokenServer := &Server{
        tokens: make(map[string]*Token),
    }

    filename := "/Users/akshithreddyc/Desktop/Workplace/grpc-token-manager/utils/static/tokens.yaml"
	readErr := tokenServer.ReadTokensFromYaml(filename)
	if readErr != nil {
		log.Fatalf("Failed to read tokens from YAML file: %v", err)
        return
	}

    token.RegisterTokenServiceServer(s, tokenServer)
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}

func (s *Server) ReadTokensFromYaml(filename string) error {
    fmt.Println("Starting to read tokens from:", filename)
    
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("Error reading the file %s: %v", filename, err)
		return err
	}

	var tokensList TokensList
	err = yaml.Unmarshal(data, &tokensList)
	if err != nil {
		fmt.Printf("Error unmarshalling the YAML data: %v", err)
		return err
	}

    fmt.Printf("Read %d tokens from the file", len(tokensList.Tokens))

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, token := range tokensList.Tokens {
		s.tokens[token.ID] = &token
		fmt.Printf("Added token with ID: %s to the server's tokens map", token.ID)
	}
    
	fmt.Println("Finished reading tokens")
	return nil
}
