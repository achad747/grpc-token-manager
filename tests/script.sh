#!/bin/bash

# Start the server in the background
go run ../cmd/tokenserver/main.go &
server_pid=$!

# Give the server some time to start
sleep 5

# Check if the server process is still running
if ! kill -0 $server_pid 2>/dev/null; then
  echo "Error: Server failed to start."
  exit 1
fi

# Function to create a token
create_token() {
  local token_id=$1
  echo "Creating Token ID: $token_id"
  go run ../cmd/tokenclient/main.go -method=create -id=$token_id
}

# Function to perform random read or write on a token
client_request() {
  local token_id=$1

  # Randomly decide whether to read or write
  local operation=$((RANDOM % 2))

  case $operation in
    0)  # Read request
      echo "Reading Token ID: $token_id"
      go run ../cmd/tokenclient/main.go -method=read -id=$token_id
      ;;

    1)  # Write request
      echo "Writing Token ID: $token_id"
      go run ../cmd/tokenclient/main.go -method=write -id=$token_id -name="RandomName" -low=10 -mid=20 -high=30
      ;;
  esac
}

# Function to delete a token
delete_token() {
  local token_id=$1
  echo "Deleting Token ID: $token_id"
  go run ../cmd/tokenclient/main.go -method=drop -id=$token_id
}

export -f create_token client_request delete_token

# Create all required tokens with concurrency of 10
seq 1 100 | xargs -I{} -P 10 bash -c 'create_token "$@"' _ {}
sleep 5

# Perform random reads and writes with a concurrency of 10
seq 1 100 | xargs -I{} -P 10 bash -c 'client_request "$@"' _ {}
sleep 5

# Delete all tokens with concurrency of 10
seq 1 100 | xargs -I{} -P 10 bash -c 'delete_token "$@"' _ {}

# Stop the server
kill -9 $server_pid
