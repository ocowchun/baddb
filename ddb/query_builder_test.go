package ddb

import (
	"bytes"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/ocowchun/baddb/ddb/core"
	"github.com/ocowchun/baddb/expression"
	"testing"
)

func TestSimplePredicateExpression(t *testing.T) {
	exp := "year = :year"
	keyConditionExpression, err := expression.ParseKeyConditionExpression(exp)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}

	builder := &QueryBuilder{
		KeyConditionExpression: keyConditionExpression,
		ExpressionAttributeValues: map[string]core.AttributeValue{
			":year": {
				N: aws.String("2025"),
			},
		},
		TableMetadata: &TableMetaData{
			PartitionKeySchema: &KeySchema{
				AttributeName: "year",
			},
		},
	}

	query, err := builder.BuildQuery()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if bytes.Compare(*query.PartitionKey, []byte("2025")) != 0 {
		t.Fatalf("Expected partition key to be %v, got %v", exp, *query.PartitionKey)
	}
	if query.SortKeyPredicate != nil {
		t.Fatalf("Expected sort key predicate to be nil")
	}
}

func TestSimplePredicateExpression_With_SortKey(t *testing.T) {
	type TestCase struct {
		exp        string
		leftTitle  string
		rightTitle string
		matches    []bool
	}

	testCases := []TestCase{
		{
			exp:       "year = :year AND title = :leftTitle",
			leftTitle: "Star Wars 4",
			matches:   []bool{true, false, false},
		},
		{
			exp:       "year = :year AND title > :leftTitle",
			leftTitle: "Star Wars 4",
			matches:   []bool{false, true, true},
		},
		{
			exp:       "year = :year AND title >= :leftTitle",
			leftTitle: "Star Wars 4",
			matches:   []bool{true, true, true},
		},
		{
			exp:       "year = :year AND title < :leftTitle",
			leftTitle: "Star Wars 5",
			matches:   []bool{true, false, false},
		},
		{
			exp:       "year = :year AND title <= :leftTitle",
			leftTitle: "Star Wars 5",
			matches:   []bool{true, true, false},
		},
		{
			exp:        "year = :year AND title BETWEEN :leftTitle AND :rightTitle",
			leftTitle:  "Star Wars 5",
			rightTitle: "Star Wars 6",
			matches:    []bool{false, true, true},
		},
	}

	entries := []*core.Entry{
		{
			Body: map[string]core.AttributeValue{
				"title": {
					S: aws.String("Star Wars 4"),
				},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"title": {
					S: aws.String("Star Wars 5"),
				},
			},
		},
		{
			Body: map[string]core.AttributeValue{
				"title": {
					S: aws.String("Star Wars 6"),
				},
			},
		},
	}

	for _, testCase := range testCases {
		keyConditionExpression, err := expression.ParseKeyConditionExpression(testCase.exp)
		if err != nil {
			t.Fatalf("expect no error, got %v", err)
		}

		expressionAttributeValues := map[string]core.AttributeValue{
			":year": {
				N: aws.String("1977"),
			},
			":leftTitle": {
				S: aws.String(testCase.leftTitle),
			},
		}
		if testCase.rightTitle != "" {
			expressionAttributeValues[":rightTitle"] = core.AttributeValue{
				S: aws.String(testCase.rightTitle),
			}
		}
		builder := &QueryBuilder{
			KeyConditionExpression:    keyConditionExpression,
			ExpressionAttributeValues: expressionAttributeValues,
			TableMetadata: &TableMetaData{
				PartitionKeySchema: &KeySchema{
					AttributeName: "year",
				},
				SortKeySchema: &KeySchema{
					AttributeName: "title",
				},
			},
		}

		query, err := builder.BuildQuery()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if bytes.Compare(*query.PartitionKey, []byte("1977")) != 0 {
			t.Fatalf("Expected partition key to be %v, got %v", "1977", *query.PartitionKey)
		}
		if query.SortKeyPredicate == nil {
			t.Fatalf("Expected sort key predicate to be non-nil")
		}
		pred := *query.SortKeyPredicate
		for i, entry := range entries {
			match, err := pred(entry)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if match != testCase.matches[i] {
				t.Fatalf("Exp: %s, Expected entry-%d match to be %v, got %v", testCase.exp, i, testCase.matches[i], match)
			}
		}
	}
}

func TestSimplePredicateExpression_With_GSI(t *testing.T) {
	exp := "regionCode= :regionCode"
	keyConditionExpression, err := expression.ParseKeyConditionExpression(exp)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}

	indexName := "regionCode-index"
	builder := &QueryBuilder{
		KeyConditionExpression: keyConditionExpression,
		ExpressionAttributeValues: map[string]core.AttributeValue{
			":regionCode": {
				S: aws.String("9527"),
			},
		},
		TableMetadata: &TableMetaData{
			PartitionKeySchema: &KeySchema{
				AttributeName: "year",
			},
			GlobalSecondaryIndexSettings: []GlobalSecondaryIndexSetting{
				{
					IndexName:        &indexName,
					PartitionKeyName: aws.String("regionCode"),
					SortKeyName:      aws.String("countryCode"),
				},
			},
		},
		IndexName: &indexName,
	}

	query, err := builder.BuildQuery()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if query.IndexName == nil || *query.IndexName != indexName {
		t.Fatalf("Expected index name to be %v, got %v", indexName, query.IndexName)

	}

	if bytes.Compare(*query.PartitionKey, []byte("9527")) != 0 {
		t.Fatalf("Expected partition key to be %v, got %v", exp, *query.PartitionKey)
	}
}
