package main

import (
    "context"
    "crypto/sha256"
    "encoding/binary"
    "fmt"
    "log"
    "net"
    "sync"
    "github.com/achad747/grpc-token-manager/pkg/token"


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

type Server struct {
    tokens sync.Map
}

func (s *Server) Create(ctx context.Context, req *token.TokenRequest) (*token.TokenResponse, error) {
    // Implementation for the Create method
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
