package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strings"
	"testing"
)

func TestTransactWriteItems(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	_, err = putItem(ddb, 2025, "Hello World", "your magic is mine", "1", "US")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	input := dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				ConditionCheck: &types.ConditionCheck{
					ConditionExpression: aws.String("attribute_not_exists(title)"),
					Key: map[string]types.AttributeValue{
						"year":  &types.AttributeValueMemberN{Value: "2025"},
						"title": &types.AttributeValueMemberS{Value: "Hello World 2"},
					},
					TableName: aws.String("movie"),
				},
			},
			{
				Put: &types.Put{
					Item: map[string]types.AttributeValue{
						"year":  &types.AttributeValueMemberN{Value: "2025"},
						"title": &types.AttributeValueMemberS{Value: "Hello World 0"},
					},
					TableName: aws.String("movie"),
				},
			},
			{
				Put: &types.Put{
					Item: map[string]types.AttributeValue{
						"year":  &types.AttributeValueMemberN{Value: "2025"},
						"title": &types.AttributeValueMemberS{Value: "Hello World 1"},
					},
					TableName: aws.String("movie"),
				},
			},
			{
				Delete: &types.Delete{
					Key: map[string]types.AttributeValue{
						"year":  &types.AttributeValueMemberN{Value: "2025"},
						"title": &types.AttributeValueMemberS{Value: "Hello World"},
					},
					TableName: aws.String("movie"),
				},
			},
			{
				Update: &types.Update{
					Key: map[string]types.AttributeValue{
						"year":  &types.AttributeValueMemberN{Value: "2025"},
						"title": &types.AttributeValueMemberS{Value: "Hello World 3"},
					},
					UpdateExpression: aws.String("SET message = :message"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":message": &types.AttributeValueMemberS{Value: "You are my special "},
					},
					TableName: aws.String("movie"),
				},
			},
		},
	}

	_, err = ddb.TransactWriteItems(context.Background(), &input)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	for _, item := range input.TransactItems {
		if item.Put != nil {
			getItemInput := &dynamodb.GetItemInput{
				Key: map[string]types.AttributeValue{
					"year":  &types.AttributeValueMemberN{Value: "2025"},
					"title": &types.AttributeValueMemberS{Value: item.Put.Item["title"].(*types.AttributeValueMemberS).Value},
				},
				TableName:      aws.String("movie"),
				ConsistentRead: aws.Bool(true),
			}
			getItemOutput, err := ddb.GetItem(context.Background(), getItemInput)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if len(getItemOutput.Item) == 0 {
				t.Fatalf("Expected items, got %v", len(getItemOutput.Item))
			}
		} else if item.Delete != nil {
			getItemInput := &dynamodb.GetItemInput{
				Key: map[string]types.AttributeValue{
					"year":  &types.AttributeValueMemberN{Value: "2025"},
					"title": &types.AttributeValueMemberS{Value: item.Delete.Key["title"].(*types.AttributeValueMemberS).Value},
				},
				TableName:      aws.String("movie"),
				ConsistentRead: aws.Bool(true),
			}
			getItemOutput, err := ddb.GetItem(context.Background(), getItemInput)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if len(getItemOutput.Item) != 0 {
				t.Fatalf("Expected no items, got %v", len(getItemOutput.Item))
			}

		}
	}
}

func TestTransactWriteItems_TooManyRequest(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	transactItems := make([]types.TransactWriteItem, 0)
	for i := 0; i < 120; i++ {
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				Item: map[string]types.AttributeValue{
					"year":  &types.AttributeValueMemberN{Value: "2025"},
					"title": &types.AttributeValueMemberS{Value: fmt.Sprintf("Hello World %d", i)},
				},
				TableName: aws.String("movie"),
			},
		})
	}
	input := dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}

	_, err = ddb.TransactWriteItems(context.Background(), &input)
	if err == nil {
		t.Fatalf("Expected has error, got nil")
	} else {
		if !strings.Contains(err.Error(), "Member must have length less than or equal to 100") {
			t.Fatalf("error message is unexpected, got %v", err)
		}
	}
}

func TestTransactWriteItems_DuplicatedKey(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	transactItems := make([]types.TransactWriteItem, 0)
	for i := 0; i < 3; i++ {
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				Item: map[string]types.AttributeValue{
					"year":  &types.AttributeValueMemberN{Value: "2025"},
					"title": &types.AttributeValueMemberS{Value: fmt.Sprintf("Hello World")},
				},
				TableName: aws.String("movie"),
			},
		})
	}
	input := dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}

	_, err = ddb.TransactWriteItems(context.Background(), &input)
	if err == nil {
		t.Fatalf("Expected has error, got nil")
	} else {
		if !strings.Contains(err.Error(), "Transaction request cannot include multiple operations on one item") {
			t.Fatalf("error message is unexpected, got %v", err)
		}
	}
}

func TestTransactWriteItems_ProvisionedThroughputExceeded(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 1, 1)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	transactItems := make([]types.TransactWriteItem, 0)
	for len(transactItems) <= 20 {
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				Item: map[string]types.AttributeValue{
					"year":  &types.AttributeValueMemberN{Value: "2025"},
					"title": &types.AttributeValueMemberS{Value: fmt.Sprintf("Hello World %d", len(transactItems))},
				},
				TableName: aws.String("movie"),
			},
		})
	}
	input := dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}

	_, err = ddb.TransactWriteItems(context.Background(), &input)

	if err == nil {
		t.Fatalf("Expected ProvisionedThroughputExceededException, but no error occurred")
	} else {
		var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
		if !errors.As(err, &provisionedThroughputExceededException) {
			t.Fatalf("Expected ProvisionedThroughputExceededException, got %v", err)
		}
	}
}
