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
			Run(args)
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

func Run(fileNames []string) {

	fmt.Println("starting process")
	fmt.Println("fan-out: ", fanOut)
	fmt.Println("buffer-size: ", bufferSize)
	fmt.Println("output-file: ", outputFile)

	start := time.Now()

	schema := core.Schema{}
	numberOfWorkers := len(fileNames) * fanOut
	channel := make(chan core.Line, bufferSize)

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

	elapsed := time.Since(start)
	fmt.Println("total elapsed time: ", elapsed)
	fmt.Println("schema length: ", len(bqSchema))

	fmt.Println("bigquery schema generated: ", outputFile)

	err = os.WriteFile("./"+outputFile, d, 0644)
	if err != nil {
		panic("error writing schema to output file: " + err.Error())
	}

}
