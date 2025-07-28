package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"strings"
	"testing"
)

func TestPutAndGetAndDeleteItem(t *testing.T) {
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

	getItemInput := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName: aws.String("movie"),
	}
	getItemOutput, err := ddb.GetItem(context.Background(), getItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(getItemOutput.Item) != 0 {
		t.Fatalf("Expected no items, got %v", len(getItemOutput.Item))
	}

	getItemInput = &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName:      aws.String("movie"),
		ConsistentRead: aws.Bool(true),
	}
	getItemOutput, err = ddb.GetItem(context.Background(), getItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(getItemOutput.Item) == 0 {
		t.Fatalf("Expected items, got %v", len(getItemOutput.Item))
	}

	deleteItemInput := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName: aws.String("movie"),
	}
	_, err = ddb.DeleteItem(context.Background(), deleteItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	getItemInput = &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName:      aws.String("movie"),
		ConsistentRead: aws.Bool(true),
	}
	getItemOutput, err = ddb.GetItem(context.Background(), getItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(getItemOutput.Item) != 0 {
		t.Fatalf("Expected no item, got %v", len(getItemOutput.Item))
	}

	getItemInput = &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName: aws.String("movie"),
	}
	getItemOutput, err = ddb.GetItem(context.Background(), getItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(getItemOutput.Item) == 0 {
		t.Fatalf("Expected no items, got %v", len(getItemOutput.Item))
	}
}

func TestPutItem_ProvisionedThroughputExceededException(t *testing.T) {
	shutdown := startServer()
	defer shutdown()

	var ddb *dynamodb.Client
	// minimize retries to test provisioned throughput exceeded exception
	{
		cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
		if err != nil {
			log.Fatalf("unable to load SDK config, %v", err)
		}

		ddb = dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
			options.BaseEndpoint = aws.String("http://localhost:8080")
			options.RetryMaxAttempts = 1
		})
	}

	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert items to exceed provisioned throughput
	for i := 0; i < 1000; i++ {
		putItemInput := &dynamodb.PutItemInput{
			Item: map[string]types.AttributeValue{
				"year":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", 2025+i)},
				"title":   &types.AttributeValueMemberS{Value: fmt.Sprintf("Hello World %d", i)},
				"message": &types.AttributeValueMemberS{Value: "your magic is mine"},
			},
			TableName: aws.String("movie"),
		}

		_, err := ddb.PutItem(context.Background(), putItemInput)
		if err != nil {
			var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
			if errors.As(err, &provisionedThroughputExceededException) {
				// Expected error
				return
			}
			t.Fatalf("Expected ProvisionedThroughputExceededException, got %v", err)
		}
	}

	t.Fatalf("Expected ProvisionedThroughputExceededException, but no error occurred")
}

// TODO: test different failure scenarios
func TestPutWithCondition(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert an item
	_, err = putItem(ddb, 2025, "Hello World", "your magic is mine", "1", "US")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Try to update item with an invalid condition
	putItemInput := &dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			"year":       &types.AttributeValueMemberN{Value: "2025"},
			"title":      &types.AttributeValueMemberS{Value: "Hello World"},
			"regionCode": &types.AttributeValueMemberS{Value: "1"},
		},
		TableName:           aws.String("movie"),
		ConditionExpression: aws.String("attribute_not_exists(#title)"),
	}

	_, err = ddb.PutItem(context.Background(), putItemInput)
	if err == nil {
		t.Fatalf("Expected Validation error, got nil")
	} else {
		if !strings.Contains(err.Error(), "An expression attribute name used in the document path is not defined; attribute name: #title") {
			t.Fatalf("error message is unexpected, got %v", err)
		}
	}

	// Try to update item with a condition that fails
	putItemInput = &dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			"year":       &types.AttributeValueMemberN{Value: "2025"},
			"title":      &types.AttributeValueMemberS{Value: "Hello World"},
			"regionCode": &types.AttributeValueMemberS{Value: "1"},
		},
		TableName:           aws.String("movie"),
		ConditionExpression: aws.String("attribute_not_exists(title)"),
	}

	_, err = ddb.PutItem(context.Background(), putItemInput)
	if err == nil {
		t.Fatalf("Expected ConditionalCheckFailedException, got nil")
	} else {
		var conditionalCheckFailedException *types.ConditionalCheckFailedException
		if !errors.As(err, &conditionalCheckFailedException) {
			t.Fatalf("Expected ConditionalCheckFailedException, got %v", err)
		}
	}

	// Try to updated the item with a condition that passes
	putItemInput = &dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			"year":       &types.AttributeValueMemberN{Value: "2025"},
			"title":      &types.AttributeValueMemberS{Value: "Hello World"},
			"message":    &types.AttributeValueMemberS{Value: "Jobs done"},
			"regionCode": &types.AttributeValueMemberS{Value: "1"},
		},
		TableName:           aws.String("movie"),
		ConditionExpression: aws.String("attribute_not_exists(#foo) AND contains(#message, :message)"),
		ExpressionAttributeNames: map[string]string{
			"#foo":     "foo",
			"#message": "message",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":message": &types.AttributeValueMemberS{Value: "magic"},
		},
	}

	_, err = ddb.PutItem(context.Background(), putItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// confirm the item is updated
	getItemInput := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName:      aws.String("movie"),
		ConsistentRead: aws.Bool(true),
	}
	getItemOutput, err := ddb.GetItem(context.Background(), getItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if val, ok := getItemOutput.Item["message"]; !ok {
		t.Fatalf("Expected message to be present, got nil")
	} else {
		if val.(*types.AttributeValueMemberS).Value != "Jobs done" {
			t.Fatalf("Expected message to be Jobs done, got %s", val.(*types.AttributeValueMemberS).Value)
		}
	}
}

func TestDelete_ProvisionedThroughputExceededException(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 100, 100)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	items := make([]map[string]types.AttributeValue, 0)
	// Insert test data
	for i := 0; i < 50; i++ {
		item, err := putItem(ddb, 2025, fmt.Sprintf("Hello World %d", i), "message", "1", fmt.Sprintf("code%d", i))
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		items = append(items, item)
	}
	err = updateProvisionedThroughput(ddb, 1, 1)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	for _, item := range items {
		deleteItemInput := &dynamodb.DeleteItemInput{
			Key: map[string]types.AttributeValue{
				"year":  item["year"],
				"title": item["title"],
			},
			TableName: aws.String("movie"),
		}
		_, err = ddb.DeleteItem(context.Background(), deleteItemInput)
		if err != nil {
			var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
			if errors.As(err, &provisionedThroughputExceededException) {
				// Expected error
				return
			}
			t.Fatalf("Expected ProvisionedThroughputExceededException, got %v", err)
		}
	}

	t.Fatalf("Expected ProvisionedThroughputExceededException, but no error occurred")
}

func TestDeleteWithCondition(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert an item
	_, err = putItem(ddb, 2025, "Hello World", "Initial message", "1", "US")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Try to delete item with a condition that fails
	deleteItemInput := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName:           aws.String("movie"),
		ConditionExpression: aws.String("attribute_not_exists(title)"),
	}

	_, err = ddb.DeleteItem(context.Background(), deleteItemInput)
	if err == nil {
		t.Fatalf("Expected ConditionalCheckFailedException, got nil")
	} else {
		var conditionalCheckFailedException *types.ConditionalCheckFailedException
		if !errors.As(err, &conditionalCheckFailedException) {
			t.Fatalf("Expected ConditionalCheckFailedException, got %v", err)
		}
	}

	// Try to delete item with a condition that passes
	deleteItemInput = &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName:           aws.String("movie"),
		ConditionExpression: aws.String("attribute_exists(title)"),
	}

	_, err = ddb.DeleteItem(context.Background(), deleteItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Confirm the item is deleted
	getItemInput := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName:      aws.String("movie"),
		ConsistentRead: aws.Bool(true),
	}
	getItemOutput, err := ddb.GetItem(context.Background(), getItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(getItemOutput.Item) != 0 {
		t.Fatalf("Expected no item, got %v", len(getItemOutput.Item))
	}
}

func TestUpdateItem(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert an item
	_, err = putItem(ddb, 2025, "Hello World", "Initial message", "1", "US")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Update the item
	updateItemInput := &dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName:        aws.String("movie"),
		UpdateExpression: aws.String("SET message = :newMessage"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newMessage": &types.AttributeValueMemberS{Value: "Updated message"},
		},
	}

	_, err = ddb.UpdateItem(context.Background(), updateItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Confirm the item is updated
	getItemInput := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2025"},
			"title": &types.AttributeValueMemberS{Value: "Hello World"},
		},
		TableName:      aws.String("movie"),
		ConsistentRead: aws.Bool(true),
	}
	getItemOutput, err := ddb.GetItem(context.Background(), getItemInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if val, ok := getItemOutput.Item["message"]; !ok {
		t.Fatalf("Expected message to be present, got nil")
	} else {
		if val.(*types.AttributeValueMemberS).Value != "Updated message" {
			t.Fatalf("Expected message to be 'Updated message', got %s", val.(*types.AttributeValueMemberS).Value)
		}
	}
}

func TestUpdate_ProvisionedThroughputExceededException(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 1, 1)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert an item
	_, err = putItem(ddb, 2025, "Hello World", "Initial message", "1", "US")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	for i := 0; i < 1000; i++ {
		updateItemInput := &dynamodb.UpdateItemInput{
			Key: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberN{Value: "2025"},
				"title": &types.AttributeValueMemberS{Value: "Hello World"},
			},
			TableName:        aws.String("movie"),
			UpdateExpression: aws.String("SET message = :newMessage"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":newMessage": &types.AttributeValueMemberS{Value: fmt.Sprintf("Updated message %d", i)},
			},
		}

		_, err = ddb.UpdateItem(context.Background(), updateItemInput)
		if err != nil {
			var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
			if errors.As(err, &provisionedThroughputExceededException) {
				// Expected error
				return
			}
			t.Fatalf("Expected ProvisionedThroughputExceededException, got %v", err)
		}
	}

	t.Fatalf("Expected ProvisionedThroughputExceededException, but no error occurred")
}
