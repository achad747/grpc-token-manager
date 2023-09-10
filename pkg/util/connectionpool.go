package util

import (
	"sync"

	"google.golang.org/grpc"
)

type ConnectionPool struct {
	mu                   sync.Mutex
	connections          map[string]chan *grpc.ClientConn
	connectionCounts     map[string]int
	cond                 *sync.Cond
	maxConnectionsPerAddr int
}

func NewConnectionPool(maxPerAddr int) *ConnectionPool {
	return &ConnectionPool{
		connections:          make(map[string]chan *grpc.ClientConn),
		connectionCounts:     make(map[string]int),
		cond:                 sync.NewCond(&sync.Mutex{}),
		maxConnectionsPerAddr: maxPerAddr,
	}
}

func (cp *ConnectionPool) GetConn(address string) (*grpc.ClientConn, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.connections[address] == nil {
		cp.connections[address] = make(chan *grpc.ClientConn, cp.maxConnectionsPerAddr)
	}

	if len(cp.connections[address]) > 0 {
		return <-cp.connections[address], nil
	}

	if cp.connectionCounts[address] < cp.maxConnectionsPerAddr {
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		cp.connectionCounts[address]++
		return conn, nil
	}

	cp.cond.L.Lock()
	for len(cp.connections[address]) == 0 && cp.connectionCounts[address] >= cp.maxConnectionsPerAddr {
		cp.cond.Wait()
	}
	cp.cond.L.Unlock()

	return <-cp.connections[address], nil
}

func (cp *ConnectionPool) ReturnConn(address string, conn *grpc.ClientConn) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.connections[address] == nil {
		cp.connections[address] = make(chan *grpc.ClientConn, cp.maxConnectionsPerAddr)
	}

    select {
		case cp.connections[address] <- conn:
			cp.cond.Broadcast()
		default:
			// Handle the situation where the channel is full
			conn.Close()
			cp.connectionCounts[address]--
	}
}

func (cp *ConnectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, conns := range cp.connections {
		close(conns)
		for conn := range conns {
			conn.Close()
		}
	}
}