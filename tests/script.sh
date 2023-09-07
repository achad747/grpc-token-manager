#!/bin/bash

client_request() {
  local client_id=$1
  
  # Randomly select a Token ID from the collection Token1 to Token100
  local token_id="Token$((RANDOM % 100 + 1))"

  echo "Client $client_id using Token ID: $token_id"

  # Randomly decide which operation to execute
  local operation=$((RANDOM % 4))

  case $operation in
    0)  # Create request
      echo "Client $client_id: Create"
      go run ../cmd/tokenclient/main.go -method=create -id=$token_id
      ;;

    1)  # Write request
      echo "Client $client_id: Write"
      go run ../cmd/tokenclient/main.go -method=write -id=$token_id -name="RandomName" -low=10 -mid=20 -high=30
      ;;

    2)  # Read request
      echo "Client $client_id: Read"
      go run ../cmd/tokenclient/main.go -method=read -id=$token_id
      ;;

    3)  # Drop request
      echo "Client $client_id: Drop"
      go run ../cmd/tokenclient/main.go -method=drop -id=$token_id
      ;;
  esac
}

export -f client_request

# Run client_request function for each client with a concurrency of 100
seq 1 1000 | xargs -I{} -P 100 bash -c 'client_request "$@"' _ {}
