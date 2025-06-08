package scan

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/ocowchun/baddb/ddb/core"
	"testing"
)

func TestBuildComparatorCondition(t *testing.T) {
	entries := []*core.Entry{
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2024")},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2025")},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2026")},
			},
		},
	}

	tests := []struct {
		filter   string
		expected []bool
	}{
		{
			filter:   "createdYear = :createdYear",
			expected: []bool{false, true, false},
		},
		{
			filter:   "createdYear < :createdYear",
			expected: []bool{true, false, false},
		},
		{
			filter:   "createdYear <= :createdYear",
			expected: []bool{true, true, false},
		},
		{
			filter:   "createdYear > :createdYear",
			expected: []bool{false, false, true},
		},
		{
			filter:   "createdYear >= :createdYear",
			expected: []bool{false, true, true},
		},
	}

	for _, tt := range tests {

		builder := &RequestBuilder{
			FilterExpressionStr:      aws.String(tt.filter),
			ExpressionAttributeNames: make(map[string]string),
			ExpressionAttributeValues: map[string]core.AttributeValue{
				":createdYear": {N: aws.String("2025")},
			},
			TableMetadata: &core.TableMetaData{
				Name: "test_table",
			},
		}
		scanReq, err := builder.Build()
		if err != nil {
			t.Fatalf("unexpected error: %v when build scan request with filter %s", err, tt.filter)
		}

		for i, entry := range entries {
			result, err := scanReq.Filter.Check(entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.expected[i] {
				t.Fatalf("expected %v but got %v for condition %s", tt.expected, result, tt.filter)
			}

		}
	}
}
