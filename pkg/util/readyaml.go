package util

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

func ReadTokensFromYaml(filename string, address string) (TokensList, error) {
    fmt.Println("Starting to read tokens from:", filename)
    
	var tokensList TokensList

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("Error reading the file %s: %v", filename, err)
		return tokensList, err
	}
	
	err = yaml.Unmarshal(data, &tokensList)
	if err != nil {
		fmt.Printf("Error unmarshalling the YAML data: %v", err)
		return tokensList, err
	}

	fmt.Println("Finished reading tokens")
	return tokensList, err
}

func Contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}