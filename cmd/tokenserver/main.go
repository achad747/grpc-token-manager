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

	minValue := uint64(1<<63 - 1)
	for x := tokenData.Domain.Low; x < tokenData.Domain.Mid; x++ {
		currentHash := Hash(tokenData.Name, x)
		if currentHash < minValue {
			minValue = currentHash
		}
	}
	tokenData.State.Partial = minValue

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

	minValue := uint64(1<<63 - 1)
	for x := tokenData.Domain.Mid; x < tokenData.Domain.High; x++ {
		currentHash := Hash(tokenData.Name, x)
		if currentHash < minValue {
			minValue = currentHash
		}
	}

	if minValue < tokenData.State.Partial {
		tokenData.State.Final = minValue
	} else {
		tokenData.State.Final = tokenData.State.Partial
	}

    return &token.TokenResponse{FinalValue: tokenData.State.Final, Status: "success"}, nil
}

func Hash(name string, nonce uint64) uint64 {
    hasher := sha256.New()
    hasher.Write([]byte(fmt.Sprintf("%s %d", name, nonce)))
    return binary.BigEndian.Uint64(hasher.Sum(nil))
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
