package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"sync"

	token "github.com/achad747/grpc-token-manager/pkg/api"
	"gopkg.in/yaml.v2"

	"google.golang.org/grpc"
)

var grpcAddress string

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

    fmt.Println("Create request received Request - %v", req)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tokens[req.GetId()]; exists {
		return nil, fmt.Errorf("Token with ID %s already exists", req.GetId())
	}

	s.tokens[req.GetId()] = &Token{
		ID: req.GetId(),
	}

    fmt.Println("Token created with id - %s", req.GetId())

    return &token.TokenResponse{Status: "success"}, nil
}

func (s *Server) Drop(ctx context.Context, req *token.TokenRequest) (*token.TokenResponse, error) {
    fmt.Println("Drop request received Request - %v", req)

    if req.GetId() == "" {
		return nil, fmt.Errorf("ID must not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tokens[req.GetId()]; !ok {
		return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
	}

    delete(s.tokens, req.GetId())
    
    fmt.Println("Token dropped with id - %s", req.GetId())

    return &token.TokenResponse{Status: "success"}, nil
}

func (s *Server) Write(ctx context.Context, req *token.WriteRequest) (*token.TokenResponse, error) {
	fmt.Println("Write request received Request - %v", req)
    if req.GetId() == "" || req.GetName() == "" {
        fmt.Println("ID and Name must not be empty")
		return nil, fmt.Errorf("ID and Name must not be empty")
	}

	if req.GetLow() >= req.GetMid() || req.GetMid() >= req.GetHigh() {
        fmt.Println("Domain bounds are invalid. It should satisfy: Low <= Mid < High")
		return nil, fmt.Errorf("Domain bounds are invalid. It should satisfy: Low <= Mid < High")
	}

	s.mu.Lock()
	tokenData, ok := s.tokens[req.GetId()]
	s.mu.Unlock()
	if !ok {
        fmt.Println("Token with ID %s not found", req.GetId())
		return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
	}

    fmt.Println("Data fetched for Id - %s Object - %v", req.GetId(), tokenData)

    if req.GetName() != "" && req.GetLow() != 0 {
        tokenData.Name = req.GetName()
        tokenData.Domain.Low = req.GetLow()
        tokenData.Domain.Mid = req.GetMid()
        tokenData.Domain.High = req.GetHigh()
    }

    fmt.Println("Data for hash function Id - %s Object - %v", req.GetId(), tokenData)

    _, partialVal := FindMinHashWithNonce(tokenData, tokenData.Domain.Low, tokenData.Domain.Mid)
    _, finalValue := FindMinHashWithNonce(tokenData, tokenData.Domain.Mid, tokenData.Domain.High)

    fmt.Println("Hash results, Partial Value - %d, Final Value - %d", partialVal, finalValue)
    tokenData.State.Partial = partialVal
	tokenData.State.Final = Min(finalValue, tokenData.State.Partial)
    tokenData.Version++

    s.mu.Lock()
	s.tokens[req.GetId()] = tokenData
	s.mu.Unlock()
    fmt.Println("Data updated locally with Toke - %v", tokenData)

    var wg sync.WaitGroup
    errors := make(chan error, len(tokenData.Readers))

    for _, reader := range tokenData.Readers {
        wg.Add(1)
        go func(readerAddr string) {
            defer wg.Done()
            fmt.Println("Replicating Token - %v to %s", tokenData,readerAddr)
            err := s.sendWriteToNode(readerAddr, tokenData)
            if err != nil {
                fmt.Println("Failed to replicate Token - %v to reader: %v", tokenData,err)
                errors <- err
            }
        }(reader)
    }

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

    fmt.Println("Token fetched for Id - %s Token - %v", req.GetId(), tokenData)
    localVersion := tokenData.Version
    localValue := tokenData.State.Final

    responses := make(chan *token.WriteReplicaRequest, len(tokenData.Readers))
    errorsCh := make(chan error, len(tokenData.Readers))

    var wg sync.WaitGroup
    for _, reader := range tokenData.Readers {
        wg.Add(1)
        go func(readerAddr string, localVersion int64) {
            defer wg.Done()
            fmt.Println("Sending read request for Id - %s Address - %s",req.GetId(), readerAddr)
            resp, err := s.sendReadToNode(readerAddr, req.GetId())
            if err != nil {
                errorsCh <- err
                fmt.Println("Error in sendReadToNode for Id - %s Address - %s Error - %v", req.GetId(), readerAddr, err)
                return
            }
            responses <- resp
        }(reader, localVersion)
    }
    wg.Wait()
    close(responses)
    close(errorsCh)

    maxVersion := localVersion
    maxValue := localValue
    for resp := range responses {
        if resp.Version > maxVersion {
            maxVersion = resp.Version
            maxValue = resp.Final
        }
    }
    
    imposeRequests := make(chan error, len(tokenData.Readers))
    fmt.Println("Imposing write on Id - %s for Readers - %v", req.GetId(),tokenData.Readers)
    for _, reader := range tokenData.Readers {
        wg.Add(1)
        go func(readerAddr string) {
            defer wg.Done()
            fmt.Println("Imposing write on Address - %s Id - ", readerAddr, req.GetId())
            err := s.sendImposeToNode(readerAddr, req.GetId(), maxVersion, maxValue)
            if err != nil {
                imposeRequests <- err
                fmt.Println("Error while imposing for Id - %s Address - %s", req.GetId(), readerAddr)
                return
            }
        }(reader)
    }
    wg.Wait()
    close(imposeRequests)

    s.mu.Lock()
    if maxVersion > tokenData.Version {
        tokenData.Version = maxVersion
        tokenData.State.Final = maxValue
        s.tokens[req.GetId()] = tokenData
        fmt.Println("Updating local value with Token - %v", tokenData)
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

    fmt.Println("Fetched connection from pool")
    client := token.NewTokenServiceClient(conn)
    fmt.Println("Reading from replica for Id - %s Address - %s", tokenID, nodeAddress)
    response, err := client.ReadFromReplica(context.Background(), &token.TokenRequest{Id: tokenID}) // Use ReplicaRead here
    if err != nil {
        fmt.Println("Unable to read from replica for Id - %s Address - %s", tokenID, nodeAddress)
        return nil, err
    }

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

    fmt.Println("Imposed Token - %v for Address - %s", tmpToken, nodeAddress)

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

    fmt.Println("Connection fetched from pool for write to %s", nodeAddress)
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

    if err != nil {
        fmt.Println("Error while replicating Token - %v to Node - %s Error - %v", t, nodeAddress, err)
    }

    return err
}

func (s *Server) WriteToReplica(ctx context.Context, req *token.WriteReplicaRequest) (*token.TokenResponse, error) {
    s.mu.Lock()
    tokenData, exists := s.tokens[req.GetId()]
    s.mu.Unlock()

    if !exists {
        return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
    }

    fmt.Println("Token retrieved %v", tokenData)
    
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

    fmt.Println("Token updated %v", tokenData)

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

	if cp.connectionCounts[address] < cp.maxConnectionsPerAddr {
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		cp.connectionCounts[address]++
		return conn, nil
	}

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
    port := flag.String("port", "50051", "Port to run the server on")
    flag.Parse()

    lis, err := net.Listen("tcp", ":" + *port)
    fmt.Printf("Starting server on port :"+*port)
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }
    s := grpc.NewServer()

    tokenServer := &Server{
        tokens: make(map[string]*Token),
        connPool: NewConnectionPool(3),
    }

    filename := "/Users/akshithreddyc/Desktop/Workplace/grpc-token-manager/utils/static/tokens.yaml"
	grpcAddress = "127.0.0.1:" + *port
    readErr := tokenServer.ReadTokensFromYaml(filename, "127.0.0.1:" + *port)
	if readErr != nil {
		log.Fatalf("Failed to read tokens from YAML file: %v", err)
        return
	}

    token.RegisterTokenServiceServer(s, tokenServer)
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}

func (s *Server) ReadTokensFromYaml(filename string, address string) error {
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

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, token := range tokensList.Tokens {
        if token.Writer == address ||  contains(token.Readers, address) {

        }
		s.tokens[token.ID] = &token
	}
    
	fmt.Println("Finished reading tokens")
	return nil
}

func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}