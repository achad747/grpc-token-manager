package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"sync"

	api "github.com/achad747/grpc-token-manager/pkg/api"
	token "github.com/achad747/grpc-token-manager/pkg/util"

	"google.golang.org/grpc"
)

var grpcAddress string

type Server struct {
	api.UnimplementedTokenServiceServer
	tokens   map[string]*token.Token
	mu       sync.RWMutex
	connPool *token.ConnectionPool
}

func (s *Server) Create(ctx context.Context, req *api.TokenRequest) (*api.TokenResponse, error) {
	if req.GetId() == "" {
		return nil, fmt.Errorf("ID must not be empty")
	}

	fmt.Printf("Create request received Request - %v\n", req)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tokens[req.GetId()]; exists {
		return nil, fmt.Errorf("Token with ID %s already exists\n", req.GetId())
	}

	s.tokens[req.GetId()] = &token.Token{
		ID: req.GetId(),
	}

	fmt.Printf("Token created with id - %s\n", req.GetId())

	return &api.TokenResponse{Status: "success"}, nil
}

func (s *Server) Drop(ctx context.Context, req *api.TokenRequest) (*api.TokenResponse, error) {
	fmt.Printf("Drop request received Request - %v\n", req)

	if req.GetId() == "" {
		return nil, fmt.Errorf("ID must not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tokens[req.GetId()]; !ok {
		return nil, fmt.Errorf("Token with ID %s not found", req.GetId())
	}

	delete(s.tokens, req.GetId())

	fmt.Printf("Token dropped with id - %s\n", req.GetId())

	return &api.TokenResponse{Status: "success"}, nil
}

func (s *Server) Write(ctx context.Context, req *api.WriteRequest) (*api.TokenResponse, error) {
	fmt.Printf("Write request received Request - %v\n", req)
	if req.GetId() == "" || req.GetName() == "" {
		fmt.Printf("ID and Name must not be empty\n")
		return nil, fmt.Errorf("ID and Name must not be empty\n")
	}

	if req.GetLow() >= req.GetMid() || req.GetMid() >= req.GetHigh() {
		fmt.Printf("Domain bounds are invalid. It should satisfy: Low <= Mid < High\n")
		return nil, fmt.Errorf("Domain bounds are invalid. It should satisfy: Low <= Mid < High\n")
	}

	s.mu.RLock()
	tokenData, ok := s.tokens[req.GetId()]
	s.mu.RUnlock()
	
	if !ok {
		fmt.Printf("Token with ID %s not found\n", req.GetId())
		return nil, fmt.Errorf("Token with ID %s not found\n", req.GetId())
	}

	fmt.Printf("Data fetched for Id - %s Object - %v\n", req.GetId(), tokenData)

	if req.GetName() != "" && req.GetLow() != 0 {
		tokenData.Name = req.GetName()
		tokenData.Domain.Low = req.GetLow()
		tokenData.Domain.Mid = req.GetMid()
		tokenData.Domain.High = req.GetHigh()
	}

	fmt.Printf("Data for hash function Id - %s Object - %v\n", req.GetId(), tokenData)

	_, partialVal := token.FindMinHashWithNonce(tokenData, tokenData.Domain.Low, tokenData.Domain.Mid, 5)
	_, finalValue := token.FindMinHashWithNonce(tokenData, tokenData.Domain.Mid, tokenData.Domain.High, 5)

	fmt.Printf("Hash results, Partial Value - %d, Final Value - %d\n", partialVal, finalValue)
	tokenData.State.Partial = partialVal
	tokenData.State.Final = token.Min(finalValue, tokenData.State.Partial)
	tokenData.Version++

	s.mu.Lock()
	s.tokens[req.GetId()] = tokenData
	s.mu.Unlock()
	fmt.Printf("Data updated locally with Token - %v\n", tokenData)

	var wg sync.WaitGroup
	errors := make(chan error, len(tokenData.Readers))

	for _, reader := range tokenData.Readers {
		wg.Add(1)
		go func(readerAddr string) {
			defer wg.Done()
			fmt.Printf("Replicating Token - %v to %s\n", tokenData, readerAddr)
			err := s.sendWriteToNode(readerAddr, tokenData)
			if err != nil {
				fmt.Printf("Failed to replicate Token - %v to reader: %v\n", tokenData, err)
				errors <- err
			}
		}(reader)
	}

	wg.Wait()

	close(errors)

	return &api.TokenResponse{FinalValue: tokenData.State.Final,
		PartialValue: tokenData.State.Partial, Status: "success"}, nil
}

func (s *Server) Read(ctx context.Context, req *api.TokenRequest) (*api.TokenResponse, error) {
	if req.GetId() == "" {
		return nil, fmt.Errorf("ID must not be empty\n")
	}

	s.mu.RLock()
	tokenData, ok := s.tokens[req.GetId()]
	s.mu.RUnlock()
	
	if !ok {
		return nil, fmt.Errorf("Token with ID %s not found\n", req.GetId())
	}

	fmt.Printf("Token fetched for Id - %s Token - %v\n", req.GetId(), tokenData)
	localVersion := tokenData.Version
	localValue := tokenData.State.Final

	responses := make(chan *api.WriteReplicaRequest, len(tokenData.Readers))
	errorsCh := make(chan error, len(tokenData.Readers))

	var wg sync.WaitGroup
	for _, reader := range tokenData.Readers {
		wg.Add(1)
		go func(readerAddr string, localVersion int64) {
			defer wg.Done()
			fmt.Printf("Sending read request for Id - %s Address - %s\n", req.GetId(), readerAddr)
			resp, err := s.sendReadToNode(readerAddr, req.GetId())
			if err != nil {
				errorsCh <- err
				fmt.Printf("Error in sendReadToNode for Id - %s Address - %s Error - %v\n", req.GetId(), readerAddr, err)
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
	fmt.Printf("Imposing write on Id - %s for Readers - %v\n", req.GetId(), tokenData.Readers)
	for _, reader := range tokenData.Readers {
		wg.Add(1)
		go func(readerAddr string) {
			defer wg.Done()
			fmt.Printf("Imposing write on Address - %s Id - %s\n", readerAddr, req.GetId())
			err := s.sendImposeToNode(readerAddr, req.GetId(), maxVersion, maxValue)
			if err != nil {
				imposeRequests <- err
				fmt.Printf("Error while imposing for Id - %s Address - %s\n", req.GetId(), readerAddr)
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
		fmt.Printf("Updating local value with Token - %v\n", tokenData)
	}
	s.mu.Unlock()

	return &api.TokenResponse{FinalValue: tokenData.State.Final,
		PartialValue: tokenData.State.Partial, Status: "success"}, nil
}

func (s *Server) sendReadToNode(nodeAddress string, tokenID string) (*api.WriteReplicaRequest, error) {
	conn, err := s.connPool.GetConn(nodeAddress)
	if err != nil {
		return nil, err
	}
	defer s.connPool.ReturnConn(nodeAddress, conn)

	fmt.Printf("Fetched connection from pool\n")
	client := api.NewTokenServiceClient(conn)
	fmt.Printf("Reading from replica for Id - %s Address - %s\n", tokenID, nodeAddress)
	response, err := client.ReadFromReplica(context.Background(), &api.TokenRequest{Id: tokenID}) // Use ReplicaRead here
	if err != nil {
		fmt.Printf("Unable to read from replica for Id - %s Address - %s\n", tokenID, nodeAddress)
		return nil, err
	}

	returnToken := &api.WriteReplicaRequest{
		Id:      response.Id,
		Name:    response.Name,
		Low:     response.Low,
		Mid:     response.Mid,
		High:    response.High,
		Partial: response.Partial,
		Final:   response.Final,
		Version: response.Version,
	}
	return returnToken, nil
}

func (s *Server) ReadFromReplica(ctx context.Context, req *api.TokenRequest) (*api.WriteReplicaRequest, error) {
	s.mu.RLock()
	tokenData, ok := s.tokens[req.GetId()]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("Token with ID %s not found\n", req.GetId())
	}

	return &api.WriteReplicaRequest{
		Id:      tokenData.ID,
		Name:    tokenData.Name,
		Low:     tokenData.Domain.Low,
		Mid:     tokenData.Domain.Mid,
		High:    tokenData.Domain.High,
		Partial: tokenData.State.Partial,
		Final:   tokenData.State.Final,
		Version: tokenData.Version,
	}, nil
}

func (s *Server) sendImposeToNode(nodeAddress string, tokenID string, version int64, value uint64) error {
	tmpToken := &token.Token{
		ID:      tokenID,
		Version: version,
		State:   token.State{Final: value},
	}

	fmt.Printf("Imposed Token - %v for Address - %s\n", tmpToken, nodeAddress)

	return s.sendWriteToNode(nodeAddress, tmpToken)
}

func (s *Server) sendWriteToNode(nodeAddress string, t *token.Token) error {
	conn, err := s.connPool.GetConn(nodeAddress)
	if err != nil {
		return err
	}
	defer s.connPool.ReturnConn(nodeAddress, conn)

	fmt.Printf("Connection fetched from pool for write to %s\n", nodeAddress)
	client := api.NewTokenServiceClient(conn)

	_, err = client.WriteToReplica(context.Background(), &api.WriteReplicaRequest{
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
		fmt.Printf("Error while replicating Token - %v to Node - %s Error - %v\n", t, nodeAddress, err)
	}

	return err
}

func (s *Server) WriteToReplica(ctx context.Context, req *api.WriteReplicaRequest) (*api.TokenResponse, error) {
	s.mu.Lock()
	tokenData, exists := s.tokens[req.GetId()]
	s.mu.Unlock()

	if !exists {
		return nil, fmt.Errorf("Token with ID %s not found\n", req.GetId())
	}

	fmt.Printf("Token retrieved %v\n", tokenData)

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

	fmt.Printf("Token updated %v\n", tokenData)

	return &api.TokenResponse{Status: "success"}, nil
}

func main() {
	port := flag.String("port", "50051", "Port to run the server on")
	flag.Parse()

	lis, err := net.Listen("tcp", ":"+*port)
	fmt.Printf("Starting server on port : %s\n", *port)
	if err != nil {
		fmt.Errorf("Failed to listen: %v\n", err)
	}
	s := grpc.NewServer()

	tokenServer := &Server{
		tokens:   make(map[string]*token.Token),
		connPool: token.NewConnectionPool(5),
	}

	filename := "/Users/akshithreddyc/Desktop/Workplace/grpc-token-manager/static/tokens.yaml"
	grpcAddress = "127.0.0.1:" + *port
	tokenList, readErr := token.ReadTokensFromYaml(filename, "127.0.0.1:"+*port)

	tokenServer.mu.Lock()
	for _, temp := range tokenList.Tokens {
		if temp.Writer == grpcAddress || token.Contains(temp.Readers, grpcAddress) {
			tokenServer.tokens[temp.ID] = &temp
		}
	}
	tokenServer.mu.Unlock()

	if readErr != nil {
		fmt.Printf("Failed to read tokens from YAML file: %v\n", err)
		return
	}

	api.RegisterTokenServiceServer(s, tokenServer)
	if err := s.Serve(lis); err != nil {
		fmt.Errorf("Failed to serve: %v\n", err)
	}
}
