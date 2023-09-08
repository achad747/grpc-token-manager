package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

type Token struct {
	ID      string   `yaml:"id"`
	Name    string   `yaml:"name"`
	Domain  Domain   `yaml:"domain"`
	State   State    `yaml:"state"`
	Version int64    `yaml:"version"`
	Writer  string   `yaml:"writer"`
	Readers []string `yaml:"readers"`
}

type Domain struct {
	Low  uint64 `yaml:"low"`
	Mid  uint64 `yaml:"mid"`
	High uint64 `yaml:"high"`
}

type State struct {
	Partial uint64 `yaml:"partial"`
	Final   uint64 `yaml:"final"`
}

type TokensList struct {
	Tokens []Token `yaml:"tokens"`
}

func main() {
	rand.Seed(time.Now().UnixNano())

	nodes := []string{
		"127.0.0.1:5001", "127.0.0.1:5002", "127.0.0.1:5003",
		"127.0.0.1:5004", "127.0.0.1:5005", "127.0.0.1:5006",
		"127.0.0.1:5007", "127.0.0.1:5008", "127.0.0.1:5009",
		"127.0.0.1:5010",
	}

	var tokens TokensList
	for i := 1; i <= 100; i++ {
		shuffledNodes := shuffle(nodes)
		writer := shuffledNodes[0]
		readers := shuffledNodes[1:4]

		domain := Domain{
			Low: uint64(rand.Intn(500)), 
			Mid: uint64(rand.Intn(500) + 500), 
			High: uint64(rand.Intn(500) + 1000),
		}
		state := State{
			Partial: 0, 
			Final: 0,
		}
		token := Token{
			ID:      fmt.Sprintf("Token%d", i),
			Name:    fmt.Sprintf("TokenName%d", i),
			Domain:  domain,
			State:   state,
			Version: 1,
			Writer:  writer,
			Readers: readers,
		}
		tokens.Tokens = append(tokens.Tokens, token)
	}

	data, err := yaml.Marshal(tokens)
	if err != nil {
		panic(err)
	}

	file, err := os.Create("tokens.yaml")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	file.Write(data)
}

func shuffle(src []string) []string {
	final := make([]string, len(src))
	perm := rand.Perm(len(src))
	for i, v := range perm {
		final[v] = src[i]
	}
	return final
}
