package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
)

func TestQuery(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 100, 100)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	items := make([]map[string]types.AttributeValue, 0)
	// Insert test data
	for i := 0; i < 4; i++ {
		item, err := putItem(ddb, 2025, fmt.Sprintf("Hello World %d", i), "message", "1", fmt.Sprintf("code%d", i))
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		items = append(items, item)
	}

	// Test query with ScanIndexForward true
	{
		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("movie"),
			KeyConditionExpression: aws.String("#year = :year"),
			ExpressionAttributeNames: map[string]string{
				"#year": "year",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":year": &types.AttributeValueMemberN{Value: "2025"},
			},
			ScanIndexForward: aws.Bool(true),
			Limit:            aws.Int32(2),
			ConsistentRead:   aws.Bool(true),
		}

		queryOutput, err := ddb.Query(context.Background(), queryInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(queryOutput.Items) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(queryOutput.Items))
		}
		assertPrimaryKey(queryOutput.Items[0], items[0], t)
		assertPrimaryKey(queryOutput.Items[1], items[1], t)
	}

	// Test query with ScanIndexForward false
	{
		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("movie"),
			KeyConditionExpression: aws.String("#year = :year"),
			ExpressionAttributeNames: map[string]string{
				"#year": "year",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":year": &types.AttributeValueMemberN{Value: "2025"},
			},
			ScanIndexForward: aws.Bool(false),
			Limit:            aws.Int32(2),
			ConsistentRead:   aws.Bool(true),
		}

		queryOutput, err := ddb.Query(context.Background(), queryInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(queryOutput.Items) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(queryOutput.Items))
		}
		assertPrimaryKey(queryOutput.Items[0], items[3], t)
		assertPrimaryKey(queryOutput.Items[1], items[2], t)
	}

	// Test query with ExclusiveStartKey
	{
		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("movie"),
			KeyConditionExpression: aws.String("#year = :year"),
			ExpressionAttributeNames: map[string]string{
				"#year": "year",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":year": &types.AttributeValueMemberN{Value: "2025"},
			},
			ScanIndexForward: aws.Bool(true),
			Limit:            aws.Int32(2),
			ConsistentRead:   aws.Bool(true),
			ExclusiveStartKey: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberN{Value: "2025"},
				"title": &types.AttributeValueMemberS{Value: "Hello World 1"},
			},
		}

		queryOutput, err := ddb.Query(context.Background(), queryInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(queryOutput.Items) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(queryOutput.Items))
		}
		assertPrimaryKey(queryOutput.Items[0], items[2], t)
		assertPrimaryKey(queryOutput.Items[1], items[3], t)
	}
}

// th

func TestQuery_ProvisionedThroughputExceededException(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 1, 20)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert test data
	for i := 0; i < 10; i++ {
		_, err := putItem(ddb, 2025, fmt.Sprintf("Hello World %d", i), "message", "1", fmt.Sprintf("code%d", i))
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	}

	// Test query with ScanIndexForward true
	{
		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("movie"),
			KeyConditionExpression: aws.String("#year = :year"),
			ExpressionAttributeNames: map[string]string{
				"#year": "year",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":year": &types.AttributeValueMemberN{Value: "2025"},
			},
			ScanIndexForward: aws.Bool(true),
			Limit:            aws.Int32(2),
			ConsistentRead:   aws.Bool(true),
		}

		_, err := ddb.Query(context.Background(), queryInput)

		if err != nil {
			var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
			if errors.As(err, &provisionedThroughputExceededException) {
				// Expected error
				return
			}
			t.Fatalf("Expected ProvisionedThroughputExceededException, got %v", err)
		} else {
			t.Fatalf("Expected ProvisionedThroughputExceededException, but no error occurred")
		}
	}
}

func TestQueryWithGsi(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	updateTestTableMetadata(ddb, 0, 0, 0)

	// Insert test data
	items := make([]map[string]types.AttributeValue, 0)
	for i := 0; i < 4; i++ {
		item, err := putItem(ddb, 2025, fmt.Sprintf("Hello World %d", i), "message", "1", fmt.Sprintf("code%d", i))
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		items = append(items, item)
	}

	// Test query with ScanIndexForward true
	{
		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("movie"),
			KeyConditionExpression: aws.String("regionCode = :regionCode"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":regionCode": &types.AttributeValueMemberS{Value: "1"},
			},
			ScanIndexForward: aws.Bool(true),
			Limit:            aws.Int32(2),
			IndexName:        aws.String("regionGSI"),
		}
		queryOutput, err := ddb.Query(context.Background(), queryInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if len(queryOutput.Items) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(queryOutput.Items))
		}
		assertPrimaryKey(queryOutput.Items[0], items[0], t)
		assertPrimaryKey(queryOutput.Items[1], items[1], t)

	}

	// Test query with ScanIndexForward false
	{
		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("movie"),
			KeyConditionExpression: aws.String("regionCode = :regionCode"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":regionCode": &types.AttributeValueMemberS{Value: "1"},
			},
			ScanIndexForward: aws.Bool(false),
			Limit:            aws.Int32(2),
			IndexName:        aws.String("regionGSI"),
		}
		queryOutput, err := ddb.Query(context.Background(), queryInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if len(queryOutput.Items) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(queryOutput.Items))
		}
		assertPrimaryKey(queryOutput.Items[0], items[3], t)
		assertPrimaryKey(queryOutput.Items[1], items[2], t)
	}

	// Test query with ExclusiveStartKey
	{
		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("movie"),
			KeyConditionExpression: aws.String("regionCode = :regionCode"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":regionCode": &types.AttributeValueMemberS{Value: "1"},
			},
			ScanIndexForward: aws.Bool(true),
			Limit:            aws.Int32(2),
			IndexName:        aws.String("regionGSI"),
			ExclusiveStartKey: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberN{Value: "2025"},
				"title": &types.AttributeValueMemberS{Value: "Hello World 1"},
			},
		}
		queryOutput, err := ddb.Query(context.Background(), queryInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if len(queryOutput.Items) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(queryOutput.Items))
		}
		assertPrimaryKey(queryOutput.Items[0], items[2], t)
		assertPrimaryKey(queryOutput.Items[1], items[3], t)
	}

	// Test query with SortKeyPredicate
	{
		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("movie"),
			KeyConditionExpression: aws.String("regionCode = :regionCode AND countryCode = :countryCode"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":regionCode":  &types.AttributeValueMemberS{Value: "1"},
				":countryCode": &types.AttributeValueMemberS{Value: "code2"},
			},
			ScanIndexForward: aws.Bool(true),
			Limit:            aws.Int32(2),
			IndexName:        aws.String("regionGSI"),
		}
		queryOutput, err := ddb.Query(context.Background(), queryInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if len(queryOutput.Items) != 1 {
			t.Fatalf("Expected 1 items, got %d", len(queryOutput.Items))
		}
		assertPrimaryKey(queryOutput.Items[0], items[2], t)
	}
}

// TODO: check GSI's billing mode is PROVISIONED
func TestQueryWithGsi_ProvisionedThroughputExceededException(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 1, 20)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert test data
	//items := make([]map[string]types.AttributeValue, 0)
	for i := 0; i < 20; i++ {
		_, err := putItem(ddb, 2025, fmt.Sprintf("Hello World %d", i), "message", "1", fmt.Sprintf("code%d", i))
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		//items = append(items, Attributes)
	}

	{
		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String("movie"),
			KeyConditionExpression: aws.String("regionCode = :regionCode"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":regionCode": &types.AttributeValueMemberS{Value: "1"},
			},
			ScanIndexForward: aws.Bool(true),
			Limit:            aws.Int32(20),
			IndexName:        aws.String("regionGSI"),
		}

		_, err := ddb.Query(context.Background(), queryInput)

		if err != nil {
			var provisionedThroughputExceededException *types.ProvisionedThroughputExceededException
			if errors.As(err, &provisionedThroughputExceededException) {
				// Expected error
				return
			}
			t.Fatalf("Expected ProvisionedThroughputExceededException, got %v", err)
		} else {
			t.Fatalf("Expected ProvisionedThroughputExceededException, but no error occurred")
		}

	}
}
