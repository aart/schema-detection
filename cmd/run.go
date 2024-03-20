package cmd

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aart/schema-detection/core"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			fmt.Println(">schema run [fileName]. Argument missing")
		} else {
			Run(args[0])
		}
	},
}

func ScanFile(fileName string, channel chan core.Line) {
	readFile, err := os.Open(fileName)
	if err != nil {
		panic("unable to open file: " + err.Error())
	}
	defer readFile.Close()
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	lineNumber := int64(1)
	for fileScanner.Scan() {
		channel <- core.Line{TextLine: fileScanner.Text(), Trace: core.Traceback{File: fileName, Line: lineNumber}}
		lineNumber++
	}
}

func Run(fileName string) {

	start := time.Now()
	//fileNames := []string{"./ndjson/benchmark/test1.ndjson", "./ndjson/benchmark/test2.ndjson", "./ndjson/benchmark/test3.ndjson", "./ndjson/benchmark/test4.ndjson", "./ndjson/benchmark/test5.ndjson"}
	fileNames := []string{fileName}

	schema := core.Schema{}
	numberOfWorkers := len(fileNames) * 10
	channel := make(chan core.Line, 1000000)

	for _ = range numberOfWorkers {
		go core.Worker(&schema, channel)
	}

	var wg sync.WaitGroup
	for _, f := range fileNames {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ScanFile(f, channel)
		}()
	}
	wg.Wait()

	time.Sleep(1 * time.Millisecond)
	for {
		if len(channel) > 1 {
			time.Sleep(1 * time.Millisecond)
		} else {
			break
		}
	}

	bqSchema := core.GenerateBigquerySchema(schema)

	d, err := bqSchema.ToJSONFields()
	if err != nil {
		panic("fatal schema conversion to JSON: " + err.Error())
	}

	fmt.Println("bigquery schema:")
	fmt.Println(string(d)) //TODO

	elapsed := time.Since(start)
	fmt.Println("total elapsed time: ", elapsed)
	fmt.Println("schema lenght: ", len(bqSchema))
}
