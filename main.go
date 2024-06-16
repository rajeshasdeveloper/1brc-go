package main

import (
	"1brc-go/logics/advanced"
	"fmt"
	"time"
)

func main() {
	start := time.Now()

	inputDatafilePath := "data/measurements.txt"
	// inputDatafilePath := "data/measurements-min.txt"
	outputDatafilePath := "data/result.txt"

	//simple.InitialLogic(inputDatafilePath, outputDatafilePath)
	//simple.SimpleLogicWithBuffer(inputDatafilePath, outputDatafilePath)
	//advanced.CustomMmapImplementation(inputDatafilePath, outputDatafilePath)
	advanced.CustomMmapWithParallelImplementation(inputDatafilePath, outputDatafilePath)

	fmt.Println("execution took:", time.Since(start))
}
