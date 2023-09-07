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
    mutex sync.Mutex
}

type Server struct {
    token.UnimplementedTokenServiceServer
    tokens sync.Map
}

func (s *Server) Create(ctx context.Context, req *token.TokenRequest) (*token.TokenResponse, error) {
    if req.GetId() == "" {
        return nil, fmt.Errorf("ID must not be empty")
    }

    _, exists := s.tokens.LoadOrStore(req.GetId(), &TokenData{
        token: &Token{
            ID: req.GetId(),
        },
    })

    if exists {
        return nil, fmt.Errorf("Token with ID %s already exists", req.GetId())
    }

    return &token.TokenResponse{Status: "success"}, nil
}

func (s *Server) Drop(ctx context.Context, req *token.TokenRequest) (*token.TokenResponse, error) {
    if req.GetId() == "" {
        return nil, fmt.Errorf("ID must not be empty")
    }

    _, ok := s.tokens.Load(req.GetId())
    if !ok {
        return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
    }

    s.tokens.Delete(req.GetId())
    return &token.TokenResponse{Status: "success"}, nil
}

func (s *Server) Write(ctx context.Context, req *token.WriteRequest) (*token.TokenResponse, error) {
    if req.GetId() == "" || req.GetName() == "" {
        return nil, fmt.Errorf("ID and Name must not be empty")
    }

    if req.GetLow() >= req.GetMid() || req.GetMid() >= req.GetHigh() {
        return nil, fmt.Errorf("Domain bounds are invalid. It should satisfy: Low <= Mid < High")
    }

    value, ok := s.tokens.Load(req.GetId())
    if !ok {
        return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
    }

    tokenData := value.(*TokenData)
    tokenData.mutex.Lock()
    defer tokenData.mutex.Unlock()

    tokenData.token.Name = req.GetName()
    tokenData.token.Domain.Low = req.GetLow()
    tokenData.token.Domain.Mid = req.GetMid()
    tokenData.token.Domain.High = req.GetHigh()

    minValue := uint64(1<<63 - 1)
    for x := tokenData.token.Domain.Low; x < tokenData.token.Domain.Mid; x++ {
        currentHash := Hash(tokenData.token.Name, x)
        if currentHash < minValue {
            minValue = currentHash
        }
    }
    tokenData.token.State.Partial = minValue

    return &token.TokenResponse{PartialValue: tokenData.token.State.Partial, Status: "success"}, nil
}

func (s *Server) Read(ctx context.Context, req *token.TokenRequest) (*token.TokenResponse, error) {
    if req.GetId() == "" {
        return nil, fmt.Errorf("ID must not be empty")
    }

    value, ok := s.tokens.Load(req.GetId())
    if !ok {
        return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
    }

    tokenData := value.(*TokenData)
    tokenData.mutex.Lock()
    defer tokenData.mutex.Unlock()

    minValue := uint64(1<<63 - 1)
    for x := tokenData.token.Domain.Mid; x < tokenData.token.Domain.High; x++ {
        currentHash := Hash(tokenData.token.Name, x)
        if currentHash < minValue {
            minValue = currentHash
        }
    }

    if minValue < tokenData.token.State.Partial {
        tokenData.token.State.Final = minValue
    } else {
        tokenData.token.State.Final = tokenData.token.State.Partial
    }

    return &token.TokenResponse{FinalValue: tokenData.token.State.Final, Status: "success"}, nil
}

func Hash(name string, nonce uint64) uint64 {
    hasher := sha256.New()
    hasher.Write([]byte(fmt.Sprintf("%s %d", name, nonce)))
    return binary.BigEndian.Uint64(hasher.Sum(nil))
}

func main() {
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }
    s := grpc.NewServer()
    token.RegisterTokenServiceServer(s, &Server{})
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
