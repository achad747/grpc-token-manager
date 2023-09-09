package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	pb "github.com/achad747/grpc-token-manager/pkg/api"
)

const (
	address = "localhost:50n051"
)

func main() {
	// Define command-line flags
	method := flag.String("method", "create", "Method to call (create, drop, write, read)")
	id := flag.String("id", "sampleID", "ID for the token request")
	name := flag.String("name", "", "Name for the write request")
	low := flag.Uint64("low", 0, "Low value for the write request")
	mid := flag.Uint64("mid", 0, "Mid value for the write request")
	high := flag.Uint64("high", 0, "High value for the write request")
	ip := flag.String("ip", "localhost", "IP Address of the server")
	port := flag.String("port", "50051", "Port of the server")

	flag.Parse()

	fmt.Println("Flags parsed ip: %s, port: %s", *ip, *port)
	address := fmt.Sprintf("%s:%s", *ip, *port)
	
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewTokenServiceClient(conn)

	fmt.Printf("Connection established to server at %s\n", address)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Use the method flag to determine which gRPC method to call
	switch *method {
	case "create":
		fmt.Println("Invoking Create method...")
		r, err := c.Create(ctx, &pb.TokenRequest{Id: *id})
		if err != nil {
			log.Fatalf("Could not create token: %v", err)
		}
		fmt.Printf("Token created with ID: %s, Response: %v\n", *id, r)
	case "drop":
		fmt.Println("Invoking Drop method...")
		r, err := c.Drop(ctx, &pb.TokenRequest{Id: *id})
		if err != nil {
			log.Fatalf("Could not drop token: %v", err)
		}
		fmt.Printf("Token dropped with ID: %s, Response: %v\n", *id, r)
	case "write":
		fmt.Println("Invoking Write method...")
		r, err := c.Write(ctx, &pb.WriteRequest{
			Id:   *id,
			Name: *name,
			Low:  *low,
			Mid:  *mid,
			High: *high,
		})
		if err != nil {
			log.Fatalf("Could not write token: %v", err)
		}
		fmt.Printf("Token written with ID: %s, Response: %v\n", *id, r)
	case "read":
		fmt.Println("Invoking Read method...")
		r, err := c.Read(ctx, &pb.TokenRequest{Id: *id})
		if err != nil {
			log.Fatalf("Could not read token: %v", err)
		}
		fmt.Printf("Token read with ID: %s, Response: %v\n", *id, r)
	default:
		log.Fatalf("Unknown method: %s", *method)
	}
}
