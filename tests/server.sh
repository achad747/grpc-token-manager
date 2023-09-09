#!/bin/bash

pids=()

# Function to handle CTRL+C (SIGINT)
cleanup() {
    echo "Caught CTRL+C. Killing all server processes..."
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null
    done
    rm -f server_pids.txt
    exit 0
}

# Set the trap
trap cleanup SIGINT

# Function to start a server on a given port
start_server() {
  local port=$1
  echo "Starting server on port: $port"
  go run ../cmd/tokenserver/main.go -port=$port &
  local server_pid=$!
  pids+=("$server_pid")
  echo $server_pid >> server_pids.txt

  # Give the server some time to start
  sleep 5

  # Check if the server process is still running
  if ! kill -0 $server_pid 2>/dev/null; then
    echo "Error: Server on port $port failed to start."
    exit 1
  fi
}

# Clear previous server pids file
rm -f server_pids.txt

# Start servers on ports 5001 to 5010
for port in {5001..5010}; do
  start_server $port
done

# Wait indefinitely to keep the script running until a CTRL+C
while true; do
    sleep 1
done
