package main

import (
    "context"
    "crypto/sha256"
    "encoding/binary"
    "fmt"
    "log"
    "net"
    "sync"
    token "github.com/achad747/grpc-token-manager/pkg/api"


    "google.golang.org/grpc"
)

type Token struct {
    ID       string
    Name     string
    Domain   struct {
        Low  uint64
        Mid  uint64
        High uint64
    }
    State struct {
        Partial uint64
        Final   uint64
    }
}

type TokenData struct {
    token *Token
}

type Server struct {
    token.UnimplementedTokenServiceServer
	tokens map[string]*Token
	mu     sync.RWMutex
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

	tokenData.Name = req.GetName()
	tokenData.Domain.Low = req.GetLow()
	tokenData.Domain.Mid = req.GetMid()
	tokenData.Domain.High = req.GetHigh()

	/*Single thread hash
    minValue := uint64(1<<63 - 1)
	for x := tokenData.Domain.Low; x < tokenData.Domain.Mid; x++ {
		currentHash := Hash(tokenData.Name, x)
		if currentHash < minValue {
			minValue = currentHash
		}
	}*/

    _, nonce := FindMinHashWithNonce(tokenData, tokenData.Domain.Mid, tokenData.Domain.High)

	tokenData.State.Partial = nonce
    tokenData.State.Final = 0

    s.mu.Lock()
	s.tokens[req.GetId()] = tokenData
	s.mu.Unlock()

    return &token.TokenResponse{PartialValue: tokenData.State.Partial, Status: "success"}, nil
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

    _, nonce := FindMinHashWithNonce(tokenData, tokenData.Domain.Mid, tokenData.Domain.High)

	//Sigle thread hash
    /*minValue := uint64(1<<63 - 1)
	for x := tokenData.Domain.Mid; x < tokenData.Domain.High; x++ {
		currentHash := Hash(tokenData.Name, x)
		if currentHash < minValue {
			minValue = currentHash
		}
	}*/

	tokenData.State.Final = Min(nonce, tokenData.State.Partial)

    s.mu.Lock()
	s.tokens[req.GetId()] = tokenData
	s.mu.Unlock()

    return &token.TokenResponse{FinalValue: tokenData.State.Final, Status: "success"}, nil
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

    token.RegisterTokenServiceServer(s, tokenServer)
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
