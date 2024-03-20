package core

import (
	"testing"

	"cloud.google.com/go/bigquery"
)

// TODO: implement more test cases
func TestSimpleInferType(t *testing.T) {
	inferredType, repeated, err := InferType(true)
	if inferredType != bigquery.BooleanFieldType || repeated || err != nil {
		t.Error("type inference failed")
	}
}
