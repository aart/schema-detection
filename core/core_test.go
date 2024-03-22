package core

import (
	"testing"

	"cloud.google.com/go/bigquery"
)

// TODO: implement more test cases

func TestRequirement(t *testing.T) {
	lines := []string{
		"{\"ts\":\"2020-06-18T10:44:12\",\"started\":{\"pid\":45678}}",
		"{\"ts\":\"2020-06-18T10:44:13\",\"logged_in\":{\"username\":\"foo\"}}",
		"{\"ts\":\"2020-06-18T10:44:13\",\"sessions_ids\":[123, 456]}"}

	schema := Schema{}
	for _, line := range lines {
		err := ProcessLine(&schema, Line{TextLine: line, Trace: Traceback{}}, 100)
		if err != nil {
			t.Error(err)
		}
	}
	if len(schema) != 4 {
		t.Error("length doesn't match")
	}

}

func TestSimpleInferType(t *testing.T) {
	inferredType, repeated, err := InferType(true)
	if inferredType != bigquery.BooleanFieldType || repeated || err != nil {
		t.Error("type inference failed")
	}
}
