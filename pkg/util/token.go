package util

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