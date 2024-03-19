package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
)

var (
	mu sync.RWMutex
)

type Schema []FieldSchema

type FieldSchema struct {
	Type     bigquery.FieldType
	Name     string
	Repeated bool
	Required bool
	Schema   Schema
	Trace    Traceback
}

type Line struct {
	TextLine string
	Trace    Traceback
}

type Traceback struct {
	File string
	Line int64
}

func GenerateBigquerySchema(schema Schema) bigquery.Schema {

	bigquerySchema := bigquery.Schema{}
	for _, f := range schema {
		bigquerySchema = append(bigquerySchema, &bigquery.FieldSchema{Name: f.Name, Type: f.Type, Repeated: f.Repeated, Required: f.Required})
	}
	return bigquerySchema
}

func InferType(value interface{}) (bigquery.FieldType, bool, error) {

	switch value.(type) {
	case bool:
		return bigquery.BooleanFieldType, false, nil
	case string:
		//TODO: try to parse as BigQuery atomic types Timestamp, Time, Date, ...
		return bigquery.StringFieldType, false, nil
	case int:
		return bigquery.IntegerFieldType, false, nil
	case float64:
		return bigquery.FloatFieldType, false, nil
	case []interface{}:
		i, ok := value.([]interface{})
		if !ok {
			return bigquery.FieldType(""), false, fmt.Errorf("type assertion error on []interface{}")
		}
		// check if all values in the array are of the same type.
		for u := range i {
			if u > 1 {
				if reflect.TypeOf(i[u]) != reflect.TypeOf(i[u-1]) {
					return bigquery.FieldType(""), false, fmt.Errorf("type inconsistency in repeated field")
				}
			}
		}
		switch i[0].(type) {
		case bool:
			return bigquery.BooleanFieldType, true, nil
		case string:
			//TODO: try to parse as BigQuery atomic types Timestamp, Time, Date, ...
			return bigquery.StringFieldType, true, nil
		case int:
			return bigquery.IntegerFieldType, true, nil
		case float64:
			return bigquery.FloatFieldType, true, nil
		}
	case interface{}:
		return bigquery.RecordFieldType, false, nil
	default:
		return bigquery.FieldType(""), false, fmt.Errorf("unsupported data type")
	}
	return bigquery.FieldType(""), false, nil
}

func Exists(schema *Schema, fieldName string) bool {
	for _, f := range *schema {
		if fieldName == f.Name {
			return true
		}
	}
	return false
}

func SyncAppend(schema *Schema, field FieldSchema) error {
	mu.Lock()
	if !Exists(schema, field.Name) {
		*schema = append(*schema, field)
	}
	mu.Unlock()
	return nil
}

func TraverseValueMap(workerID int, schema *Schema, inputMap *map[string]interface{}, trace Traceback) error {

	for k, v := range *inputMap {

		inferredType, repeated, err := InferType(v)
		if err != nil {
			return err
		}

		mu.RLock()
		if Exists(schema, k) {
			mu.RUnlock()
			continue
		}
		mu.RUnlock()

		err = SyncAppend(schema, FieldSchema{Name: k, Type: inferredType, Repeated: repeated, Trace: Traceback{File: trace.File, Line: trace.Line}})
		if err != nil {
			return err
		}

	}
	return nil
}

func WorkerProcess(workerID int, schema *Schema, channel chan Line) {
	var value interface{}
	for {
		line := <-channel
		json.Unmarshal([]byte(line.TextLine), &value)
		valueMap, ok := value.(map[string]interface{})
		if !ok {
			fmt.Println(workerID, "type assertion error on map[string]interface{}") //TODO
		}
		err := TraverseValueMap(workerID, schema, &valueMap, line.Trace)
		if err != nil {
			fmt.Println(err) //TODO
		}
	}
}

func main() {

	fileName := "./test.ndjson"

	numberOfWokers := 10
	schema := Schema{}

	channel := make(chan Line, 10000)

	for id := range numberOfWokers {
		go WorkerProcess(id, &schema, channel)
	}

	readFile, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err) //TODO
	}
	defer readFile.Close()

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	lineNumber := int64(1)
	for fileScanner.Scan() {
		channel <- Line{TextLine: fileScanner.Text(), Trace: Traceback{File: fileName, Line: lineNumber}}
		lineNumber++
	}

	time.Sleep(2 * time.Second)

	bqSchema := GenerateBigquerySchema(schema)

	d, err := bqSchema.ToJSONFields()
	if err != nil {
		fmt.Println(err) //TODO
	}

	fmt.Println(string(d)) //TODO

}
