#!/bin/bash

# Function to perform random read or write on a token
client_request() {
  local token_info=$1
  local token_id=$(echo $token_info | cut -d':' -f1)
  local writer=$(echo $token_info | cut -d':' -f2)
  local readers=$(echo $token_info | cut -d':' -f3- | tr ',' ' ')

  # Randomly decide whether to read or write
  #local operation=$((RANDOM % 2))
  local operation=1

  case $operation in
    0)  # Read request
        # Select a random reader
        local readerArray=($readers)
        local randomIndex=$((RANDOM % ${#readerArray[@]}))
        local reader=${readerArray[$randomIndex]}
        local reader_ip=$(echo $reader | cut -d':' -f1)
        local reader_port=$(echo $reader | cut -d':' -f2)
        echo "Reading Token ID: $token_id from $reader"
        go run ../cmd/tokenclient/main.go -method=read -id=$token_id -ip=$reader_ip -port=$reader_port
        ;;

    1)  # Write request
        echo "Debug: Writer value is $writer"
        echo "Debug: Raw writer value is $writer"
        local writer_ip=$(echo $writer | awk -F":" '{print $1}')
        local writer_port=$(echo $writer | awk -F":" '{print $2}')
        echo "Debug: Extracted writer_ip is $writer_ip"
        echo "Debug: Extracted writer_port is $writer_port"
        echo "Writing Token ID: $token_id to $writer"
        local low=$(( RANDOM % 3333 ))
        local mid=$(( low + 1 + RANDOM % 3333 ))
        local high=$(( mid + 1 + RANDOM % (10000 - mid)))

        echo "Writing Token ID: $token_id to $writer with values port=$writer_port low=$low, mid=$mid, high=$high"
        go run ../cmd/tokenclient/main.go -method=write -id=$token_id -name="RandomName" -low=$low -mid=$mid -high=$high -ip="localhost" -port=$writer_port
        ;;
    esac
}

# Export the client_request function
export -f client_request

# Nodes definition
nodes=(
    "localhost:5001" "localhost:5002" "localhost:5003"
    "localhost:5004" "localhost:5005" "localhost:5006"
    "localhost:5007" "localhost:5008" "localhost:5009"
    "localhost:5010"
)

# Generating tokens array based on the logic
declare -a tokens
for i in $(seq 1 100); do
    writerIndex=$(( (i-1) % ${#nodes[@]} ))
    echo "Debug: Writer index is $writerIndex"
    writer=${nodes[$writerIndex]}
    echo "Debug: Writer value (from nodes array) is ${nodes[$writerIndex]}"
    reader1=${nodes[$(( (writerIndex+1) % ${#nodes[@]} ))]}
    reader2=${nodes[$(( (writerIndex+2) % ${#nodes[@]} ))]}
    reader3=${nodes[$(( (writerIndex+3) % ${#nodes[@]} ))]}
    tokens+=( "Token${i}:$writer:$reader1,$reader2,$reader3" )
done

# Perform random reads and writes with a concurrency of 10
echo "${tokens[@]}" | tr ' ' '\n' | xargs -I{} -P 10 bash -c 'client_request "$@"' _ {} 
