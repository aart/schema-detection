package core

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/bigquery"
)

var (
	mu                   sync.RWMutex
	TotalLineCounter     atomic.Uint64
	ProcessedLineCounter atomic.Uint64
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

	mu.RLock()
	defer mu.RUnlock()

	bigquerySchema := bigquery.Schema{}
	for _, f := range schema {
		if f.Type == bigquery.RecordFieldType {
			nestedSchema := GenerateBigquerySchema(f.Schema)
			bigquerySchema = append(bigquerySchema, &bigquery.FieldSchema{Name: f.Name, Type: f.Type, Schema: nestedSchema, Repeated: f.Repeated, Required: f.Required})
		} else {
			bigquerySchema = append(bigquerySchema, &bigquery.FieldSchema{Name: f.Name, Type: f.Type, Repeated: f.Repeated, Required: f.Required})
		}
	}
	return bigquerySchema
}

func InferType(value interface{}) (bigquery.FieldType, bool, error) {

	switch value.(type) {
	case bool:
		return bigquery.BooleanFieldType, false, nil
	case string:
		return bigquery.StringFieldType, false, nil
	case int:
		return bigquery.IntegerFieldType, false, nil
	case float64:
		return bigquery.FloatFieldType, false, nil
	case []interface{}:
		i, ok := value.([]interface{})
		if !ok {
			return bigquery.FieldType(""), false, fmt.Errorf("unable to infer type: type assertion error")
		}
		switch i[0].(type) {
		case bool:
			return bigquery.BooleanFieldType, true, nil
		case string:
			return bigquery.StringFieldType, true, nil
		case int:
			return bigquery.IntegerFieldType, true, nil
		case float64:
			return bigquery.FloatFieldType, true, nil
		case interface{}:
			return bigquery.RecordFieldType, true, nil
		}
	case interface{}:
		return bigquery.RecordFieldType, false, nil
	default:
		return bigquery.FieldType(""), false, fmt.Errorf("unable to infer type")
	}
	return bigquery.FieldType(""), false, nil
}

func Exists(schema *Schema, fieldName string) bool {
	mu.RLock()
	defer mu.RUnlock()

	for _, f := range *schema {
		if fieldName == f.Name {
			return true
		}
	}
	return false
}

func MutexAppend(schema *Schema, field FieldSchema) error {
	mu.Lock()
	if !Exists(schema, field.Name) {
		*schema = append(*schema, field)
	}
	mu.Unlock()
	return nil
}

func TraverseValueMap(schema *Schema, inputMap *map[string]interface{}, trace Traceback) error {

	for k, v := range *inputMap {

		if Exists(schema, k) {
			continue
		}

		inferredType, repeated, err := InferType(v)
		if err != nil {
			return fmt.Errorf(err.Error()+" on field: %s", k)
		}

		if inferredType == bigquery.RecordFieldType && !repeated {
			nestedSchema := make(Schema, 0)
			nestedMap, ok := v.(map[string]interface{})
			if !ok {
				return fmt.Errorf("fatal type assertion on field: %s", k)
			}
			err := TraverseValueMap(&nestedSchema, &nestedMap, trace) // Recursive call
			if err != nil {
				return fmt.Errorf(err.Error()+" on field: %s", k)
			}
			err = MutexAppend(schema, FieldSchema{Name: k, Type: inferredType, Repeated: repeated, Schema: nestedSchema, Trace: trace})
			if err != nil {
				return fmt.Errorf(err.Error()+" on field: %s", k)
			}
		} else if inferredType == bigquery.RecordFieldType && repeated {
			nestedSchema := make(Schema, 0)
			array, ok := v.([]interface{})
			if !ok {
				return fmt.Errorf("fatal type assertion on repeated field: %s", k)
			}
			nestedMap, ok := array[0].(map[string]interface{})
			if !ok {
				return fmt.Errorf("fatal type assertion on repeated field: %s", k)
			}
			err := TraverseValueMap(&nestedSchema, &nestedMap, trace) // Recursive call
			if err != nil {
				return fmt.Errorf(err.Error()+" on field: %s", k)
			}
			err = MutexAppend(schema, FieldSchema{Name: k, Type: inferredType, Repeated: repeated, Schema: nestedSchema, Trace: trace})
			if err != nil {
				return fmt.Errorf(err.Error()+" on field: %s", k)
			}
		} else {
			err = MutexAppend(schema, FieldSchema{Name: k, Type: inferredType, Repeated: repeated, Trace: trace})
			if err != nil {
				return fmt.Errorf(err.Error()+" on field: %s", k)
			}
		}

	}
	return nil
}

func ProcessLine(schema *Schema, line Line, samplingPercentage int) error {
	if rand.IntN(100) < samplingPercentage {
		var value interface{}
		err := json.Unmarshal([]byte(line.TextLine), &value)
		if err != nil {
			return fmt.Errorf("fatal unmarshal at position: " + line.Trace.File + " " + strconv.FormatInt(line.Trace.Line, 10))
		}
		valueMap, ok := value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("fatal type assertion error at position: " + line.Trace.File + " " + strconv.FormatInt(line.Trace.Line, 10))
		}
		err = TraverseValueMap(schema, &valueMap, line.Trace)
		if err != nil {
			return fmt.Errorf("error: " + err.Error() + " at position: " + line.Trace.File + " " + strconv.FormatInt(line.Trace.Line, 10))
		}
		ProcessedLineCounter.Add(1)
	}
	TotalLineCounter.Add(1)
	return nil
}
