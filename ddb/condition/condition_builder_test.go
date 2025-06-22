package condition

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/ddb/expression/parser"
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
		exp      string
		expected []bool
	}{
		{
			exp:      "createdYear = :createdYear",
			expected: []bool{false, true, false},
		},
		{
			exp:      "createdYear <> :createdYear",
			expected: []bool{true, false, true},
		},
		{
			exp:      "createdYear < :createdYear",
			expected: []bool{true, false, false},
		},
		{
			exp:      "createdYear <= :createdYear",
			expected: []bool{true, true, false},
		},
		{
			exp:      "createdYear > :createdYear",
			expected: []bool{false, false, true},
		},
		{
			exp:      "createdYear >= :createdYear",
			expected: []bool{false, true, true},
		},
	}

	for _, tt := range tests {
		condition, err := BuildCondition(
			tt.exp,
			make(map[string]string),
			map[string]core.AttributeValue{
				":createdYear": {N: aws.String("2025")},
			})
		if err != nil {
			t.Fatalf("unexpected error: %v when build condition %s", err, tt.exp)
		}

		for i, entry := range entries {
			result, err := condition.Check(entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.expected[i] {
				t.Fatalf("expected %v but got %v for condition %s", tt.expected, result, tt.exp)
			}

		}
	}
}

func TestBuildBetweenCondition(t *testing.T) {
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
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2027")},
			},
		},
	}

	tests := []struct {
		exp      string
		expected []bool
	}{
		{
			exp:      "createdYear BETWEEN :start AND :end",
			expected: []bool{false, true, true, false},
		},
	}

	for _, tt := range tests {
		condition, err := BuildCondition(
			tt.exp,
			make(map[string]string),
			map[string]core.AttributeValue{
				":start": {N: aws.String("2025")},
				":end":   {N: aws.String("2026")},
			})
		if err != nil {
			t.Fatalf("unexpected error: %v when building condition %s", err, tt.exp)
		}

		for i, entry := range entries {
			result, err := condition.Check(entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.expected[i] {
				t.Fatalf("expected %v but got %v for condition %s", tt.expected[i], result, tt.exp)
			}
		}
	}
}

func TestBuildInCondition(t *testing.T) {
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
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2027")},
			},
		},
	}

	tests := []struct {
		exp      string
		expected []bool
	}{
		{
			exp:      "createdYear IN (:val1, :val2)",
			expected: []bool{false, true, true, false},
		},
	}

	for _, tt := range tests {
		condition, err := BuildCondition(
			tt.exp,
			make(map[string]string),
			map[string]core.AttributeValue{
				":val1": {N: aws.String("2025")},
				":val2": {N: aws.String("2026")},
			})
		if err != nil {
			t.Fatalf("unexpected error: %v when building condition %s", err, tt.exp)
		}

		for i, entry := range entries {
			result, err := condition.Check(entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.expected[i] {
				t.Fatalf("expected %v but got %v for condition %s", tt.expected[i], result, tt.exp)
			}
		}
	}
}

func TestBuildFunctionCondition(t *testing.T) {
	entries := []*core.Entry{
		{
			Body: map[string]core.AttributeValue{
				"fistName": {S: aws.String("Alice")},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"fistName": {S: aws.String("Bob")},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"fistName": {S: aws.String("Charlie")},
			},
		},
	}

	tests := []struct {
		exp                       string
		expressionAttributeValues map[string]core.AttributeValue
		expected                  []bool
	}{
		{
			exp:                       "attribute_exists(fistName)",
			expressionAttributeValues: map[string]core.AttributeValue{},
			expected:                  []bool{true, true, true},
		},
		{
			exp:                       "attribute_not_exists(age)",
			expressionAttributeValues: map[string]core.AttributeValue{},
			expected:                  []bool{true, true, true},
		},
		{
			exp: "begins_with(fistName, :prefix)",
			expressionAttributeValues: map[string]core.AttributeValue{
				":prefix": {S: aws.String("A")},
			},
			expected: []bool{true, false, false},
		},
		{
			exp: "contains(fistName, :substring)",
			expressionAttributeValues: map[string]core.AttributeValue{
				":substring": {S: aws.String("ar")},
			},
			expected: []bool{false, false, true},
		},
	}

	for _, tt := range tests {
		condition, err := BuildCondition(
			tt.exp,
			make(map[string]string),
			tt.expressionAttributeValues,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v when building condition %s", err, tt.exp)
		}

		for i, entry := range entries {
			result, err := condition.Check(entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.expected[i] {
				t.Fatalf("expected %v but got %v for condition %s", tt.expected[i], result, tt.exp)
			}
		}
	}
}

func TestBuildAndConditionExpression(t *testing.T) {
	entries := []*core.Entry{
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2024")},
				"fistName":    {S: aws.String("Bob")},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2025")},
				"fistName":    {S: aws.String("Bob")},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2026")},
				"fistName":    {S: aws.String("Charlie")},
			},
		},
	}

	tests := []struct {
		exp      string
		expected []bool
	}{
		{
			exp:      "createdYear = :createdYear AND fistName = :fistName",
			expected: []bool{false, true, false},
		},
		{
			exp:      "createdYear > :createdYear AND fistName = :fistName",
			expected: []bool{false, false, false},
		},
		{
			exp:      "createdYear < :createdYear AND fistName = :fistName",
			expected: []bool{true, false, false},
		},
	}

	for _, tt := range tests {
		condition, err := BuildCondition(
			tt.exp,
			make(map[string]string),
			map[string]core.AttributeValue{
				":createdYear": {N: aws.String("2025")},
				":fistName":    {S: aws.String("Bob")},
			})
		if err != nil {
			t.Fatalf("unexpected error: %v when building condition %s", err, tt.exp)
		}

		for i, entry := range entries {
			result, err := condition.Check(entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.expected[i] {
				t.Fatalf("expected %v but got %v for condition %s", tt.expected[i], result, tt.exp)
			}
		}
	}
}

func TestBuildOrConditionExpression(t *testing.T) {
	entries := []*core.Entry{
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2024")},
				"fistName":    {S: aws.String("Alice")},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2025")},
				"fistName":    {S: aws.String("Bob")},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"createdYear": {N: aws.String("2026")},
				"fistName":    {S: aws.String("Charlie")},
			},
		},
	}

	tests := []struct {
		exp      string
		expected []bool
	}{
		{
			exp:      "createdYear = :createdYear OR fistName = :fistName",
			expected: []bool{true, true, false},
		},
	}

	for _, tt := range tests {
		condition, err := BuildCondition(
			tt.exp,
			make(map[string]string),
			map[string]core.AttributeValue{
				":createdYear": {N: aws.String("2024")},
				":fistName":    {S: aws.String("Bob")},
			})
		if err != nil {
			t.Fatalf("unexpected error: %v when building condition %s", err, tt.exp)
		}

		for i, entry := range entries {
			result, err := condition.Check(entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.expected[i] {
				t.Fatalf("expected %v but got %v for condition %s", tt.expected[i], result, tt.exp)
			}
		}
	}
}

func TestBuildNotCondition(t *testing.T) {
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
		exp      string
		expected []bool
	}{
		{
			exp:      "NOT createdYear = :createdYear",
			expected: []bool{true, false, true},
		},
	}

	for _, tt := range tests {
		condition, err := BuildCondition(
			tt.exp,
			make(map[string]string),
			map[string]core.AttributeValue{
				":createdYear": {N: aws.String("2025")},
			})
		if err != nil {
			t.Fatalf("unexpected error: %v when building condition %s", err, tt.exp)
		}

		for i, entry := range entries {
			result, err := condition.Check(entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.expected[i] {
				t.Fatalf("expected %v but got %v for condition %s", tt.expected[i], result, tt.exp)
			}
		}
	}
}

func TestBuildConditionReservedWord(t *testing.T) {
	_, err := BuildCondition(
		"language = :language",
		map[string]string{}, // no ExpressionAttributeNames
		map[string]core.AttributeValue{
			":language": {N: aws.String("English")},
		},
	)

	var actualErr *parser.ReservedKeywordException
	if errors.As(err, &actualErr) {
		if actualErr.ReservedKeyword != "language" {
			t.Fatalf("expected reserved keyword 'language', got: %s", actualErr.ReservedKeyword)
		}
	} else {
		t.Fatalf("expected reserved word error, got: %v", err)
	}
}
