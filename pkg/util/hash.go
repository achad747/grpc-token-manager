package util

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

func Hash(name string, nonce uint64) uint64 {
    hasher := sha256.New()
    hasher.Write([]byte(fmt.Sprintf("%s %d", name, nonce)))
    return binary.BigEndian.Uint64(hasher.Sum(nil))
}

func Min(a, b uint64) uint64 {
    if a < b {
        return a
    }
    return b
}

func FindMinHashWithNonce(tokenData *Token, start, end uint64, numGoroutines int) (uint64, uint64) {
    chunkSize := (end - start) / uint64(numGoroutines)

    results := make(chan struct {
        hash  uint64
        nonce uint64
    }, numGoroutines)

    for i := 0; i < numGoroutines; i++ {
        currentStart := start + uint64(i)*chunkSize
        currentEnd := currentStart + chunkSize
        if i == numGoroutines-1 {
            currentEnd = end
        }

        go func(s, e uint64) {
            localMin := uint64(1<<63 - 1)
            localNonce := s
            for x := s; x < e; x++ {
                currentHash := Hash(tokenData.Name, x)
                if currentHash < localMin {
                    localMin = currentHash
                    localNonce = x
                }
            }
            results <- struct {
                hash  uint64
                nonce uint64
            }{localMin, localNonce}
        }(currentStart, currentEnd)
    }

    minValue := uint64(1<<63 - 1)
    minNonce := start

    for i := 0; i < numGoroutines; i++ {
        result := <-results
        if result.hash < minValue {
            minValue = result.hash
            minNonce = result.nonce
        }
    }

    close(results)
    return minValue, minNonce
}
