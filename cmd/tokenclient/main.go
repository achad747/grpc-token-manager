package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	pb "github.com/achad747/grpc-token-manager/pkg/token"
)

const (
	address = "localhost:50051"
)

func main() {
	// Define command-line flags
	method := flag.String("method", "create", "Method to call (create, drop, write, read)")
	id := flag.String("id", "sampleID", "ID for the token request")
	name := flag.String("name", "", "Name for the write request")
	low := flag.Uint64("low", 0, "Low value for the write request")
	mid := flag.Uint64("mid", 0, "Mid value for the write request")
	high := flag.Uint64("high", 0, "High value for the write request")

	flag.Parse()

	fmt.Println("Flags parsed successfully. Establishing connection...")

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
		fmt.Printf("Token created with ID: %s, Message: %s\n", *id, r.GetValue())
	case "drop":
		fmt.Println("Invoking Drop method...")
		r, err := c.Drop(ctx, &pb.TokenRequest{Id: *id})
		if err != nil {
			log.Fatalf("Could not drop token: %v", err)
		}
		fmt.Printf("Token dropped with ID: %s, Message: %s\n", *id, r.GetValue())
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
		fmt.Printf("Token written with ID: %s, Name: %s, Low: %d, Mid: %d, High: %d\n", *id, *name, *low, *mid, *high)
	case "read":
		fmt.Println("Invoking Read method...")
		r, err := c.Read(ctx, &pb.TokenRequest{Id: *id})
		if err != nil {
			log.Fatalf("Could not read token: %v", err)
		}
		fmt.Printf("Token read with ID: %s, Final Value: %d\n", *id, r.GetFinalValue())
	default:
		log.Fatalf("Unknown method: %s", *method)
	}
}
