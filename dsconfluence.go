package main

import (
	"fmt"

	"github.com/LF-Engineering/ds-confluence/gen/models"
	jsoniter "github.com/json-iterator/go"
)

func main() {
	data := &models.Data{}
	jsonBytes, err := jsoniter.Marshal(data)
	if err != nil {
		fmt.Printf("Error: %+v\n", err)
		return
	}
	fmt.Printf("%s\n", string(jsonBytes))
}
