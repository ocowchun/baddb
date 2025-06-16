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
	"net/http"
	"sort"
	"strings"
	"testing"
)

func TestCreateAndDeleteTable(t *testing.T) {
	shutdown := startServer()
	defer shutdown()

	ddb := newDdbClient()

	res, err := createTable(ddb, 5, 5)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if *res.TableDescription.TableName != "movie" {
		t.Fatalf("Expected table name %s, got %s", "movie", *res.TableDescription.TableName)

	}

	listTablesInput := &dynamodb.ListTablesInput{}
	listTablesOutput, err := ddb.ListTables(context.Background(), listTablesInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(listTablesOutput.TableNames) != 2 {
		t.Fatalf("Expected 2 table, got %d", len(listTablesOutput.TableNames))
	}
	sort.Strings(listTablesOutput.TableNames)
	if listTablesOutput.TableNames[0] != "baddb_table_metadata" {
		t.Fatalf("Expected table name %s, got %s", "baddb_table_metadata", listTablesOutput.TableNames[0])
	}
	if listTablesOutput.TableNames[1] != "movie" {
		t.Fatalf("Expected table name %s, got %s", "movie", listTablesOutput.TableNames[0])
	}

	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String("movie"),
	}
	describeTableOutput, err := ddb.DescribeTable(context.Background(), describeTableInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if *describeTableOutput.Table.TableName != "movie" {
		t.Fatalf("Expected table name %s, got %s", "movie", *describeTableOutput.Table.TableName)
	}

	deleteTableInput := &dynamodb.DeleteTableInput{
		TableName: aws.String("movie"),
	}
	_, err = ddb.DeleteTable(context.Background(), deleteTableInput)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	_, err = ddb.DescribeTable(context.Background(), describeTableInput)
	if err == nil {
		t.Fatalf("Expected error, got nil")
	} else {
		var resourceNotFoundException *types.ResourceNotFoundException
		if !errors.As(err, &resourceNotFoundException) {
			t.Fatalf("Expected ResourceNotFoundException, got %v", err)
		}
	}
}

func TestPutAndGetAndDeleteItem(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	_, err = putItem(ddb)

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

func TestBatchGetItem(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert test data
	items := make([]map[string]types.AttributeValue, 0)
	for i := 0; i < 4; i++ {
		putItemInput := &dynamodb.PutItemInput{
			Item: map[string]types.AttributeValue{
				"year":        &types.AttributeValueMemberN{Value: "2025"},
				"title":       &types.AttributeValueMemberS{Value: fmt.Sprintf("Hello World %d", i)},
				"regionCode":  &types.AttributeValueMemberS{Value: "1"},
				"countryCode": &types.AttributeValueMemberS{Value: fmt.Sprintf("code%d", i)},
			},
			TableName: aws.String("movie"),
		}
		items = append(items, putItemInput.Item)

		_, err := ddb.PutItem(context.Background(), putItemInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		// workaround for the eventual consistency of the local dynamodb
		_, err = ddb.PutItem(context.Background(), putItemInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	}

	// Test
	{
		input := &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				"movie": {
					Keys: []map[string]types.AttributeValue{
						{
							"year":  &types.AttributeValueMemberN{Value: "2025"},
							"title": &types.AttributeValueMemberS{Value: "Hello World 0"},
						},
						{
							"year":  &types.AttributeValueMemberN{Value: "2025"},
							"title": &types.AttributeValueMemberS{Value: "Hello World 1"},
						},
					},
					ConsistentRead: aws.Bool(true),
				},
			},
		}
		output, err := ddb.BatchGetItem(context.Background(), input)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if len(output.Responses["movie"]) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(output.Responses["movie"]))
		}

		for i, actualItem := range output.Responses["movie"] {
			assertPrimaryKey(actualItem, items[i], t)
		}

		if len(output.UnprocessedKeys["movie"].Keys) != 0 {
			t.Fatalf("Expected 0 unprocessed keys, got %d", len(output.UnprocessedKeys["movie"].Keys))
		}
	}

	// Test with unprocessed keys
	updateTestTableMetadata(ddb, 60, 60, 2)
	{
		input := &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				"movie": {
					Keys: []map[string]types.AttributeValue{
						{
							"year":  &types.AttributeValueMemberN{Value: "2025"},
							"title": &types.AttributeValueMemberS{Value: "Hello World 0"},
						},
						{
							"year":  &types.AttributeValueMemberN{Value: "2025"},
							"title": &types.AttributeValueMemberS{Value: "Hello World 1"},
						},
						{
							"year":  &types.AttributeValueMemberN{Value: "2025"},
							"title": &types.AttributeValueMemberS{Value: "Hello World 2"},
						},
					},
					ConsistentRead: aws.Bool(true),
				},
			},
		}
		output, err := ddb.BatchGetItem(context.Background(), input)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if len(output.Responses["movie"]) != 1 {
			t.Fatalf("Expected 1 items, got %d", len(output.Responses["movie"]))
		}

		assertPrimaryKey(output.Responses["movie"][0], items[2], t)

		if len(output.UnprocessedKeys["movie"].Keys) != 2 {
			t.Fatalf("Expected 2 unprocessed keys, got %d", len(output.UnprocessedKeys["movie"].Keys))
		}
	}

}

func TestBatchWriteItem(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	requests := make([]types.WriteRequest, 0)
	for i := 0; i < 4; i++ {
		req := types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"year":        &types.AttributeValueMemberN{Value: "2025"},
					"title":       &types.AttributeValueMemberS{Value: fmt.Sprintf("Hello World %d", i)},
					"regionCode":  &types.AttributeValueMemberS{Value: "1"},
					"countryCode": &types.AttributeValueMemberS{Value: fmt.Sprintf("code%d", i)},
				},
			},
		}
		requests = append(requests, req)
	}

	{
		requestItems := make(map[string][]types.WriteRequest)
		requestItems["movie"] = requests[:2]
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: requestItems,
		}

		output, err := ddb.BatchWriteItem(context.Background(), input)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(output.UnprocessedItems["movie"]) != 0 {
			t.Fatalf("Expected 0 unprocessed items, got %d", len(output.UnprocessedItems["movie"]))
		}
	}

	{
		updateTestTableMetadata(ddb, 60, 60, 2)
		requestItems := make(map[string][]types.WriteRequest)
		requestItems["movie"] = requests[:4]
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: requestItems,
		}

		output, err := ddb.BatchWriteItem(context.Background(), input)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(output.UnprocessedItems["movie"]) != 2 {
			t.Fatalf("Expected 2 unprocessed items, got %d", len(output.UnprocessedItems["movie"]))
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

	// Insert test data
	items := make([]map[string]types.AttributeValue, 0)
	for i := 0; i < 4; i++ {
		putItemInput := &dynamodb.PutItemInput{
			Item: map[string]types.AttributeValue{
				"year":        &types.AttributeValueMemberN{Value: "2025"},
				"title":       &types.AttributeValueMemberS{Value: fmt.Sprintf("Hello World %d", i)},
				"regionCode":  &types.AttributeValueMemberS{Value: "1"},
				"countryCode": &types.AttributeValueMemberS{Value: fmt.Sprintf("code%d", i)},
			},
			TableName: aws.String("movie"),
		}
		items = append(items, putItemInput.Item)

		_, err := ddb.PutItem(context.Background(), putItemInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		// workaround for the eventual consistency of the local dynamodb
		_, err = ddb.PutItem(context.Background(), putItemInput)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
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

func assertPrimaryKey(actual map[string]types.AttributeValue, expected map[string]types.AttributeValue, t *testing.T) {
	t.Helper()
	if actual["year"].(*types.AttributeValueMemberN).Value != expected["year"].(*types.AttributeValueMemberN).Value {
		t.Fatalf("Expected year to be %s, got %s", expected["year"].(*types.AttributeValueMemberN).Value, actual["year"].(*types.AttributeValueMemberN).Value)
	}
	if actual["title"].(*types.AttributeValueMemberS).Value != expected["title"].(*types.AttributeValueMemberS).Value {
		t.Fatalf("Expected title to be %s, got %s", expected["title"].(*types.AttributeValueMemberS).Value, actual["title"].(*types.AttributeValueMemberS).Value)
	}
}

func TestQuery(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	items := make([]map[string]types.AttributeValue, 0)
	// Insert test data
	for i := 0; i < 4; i++ {
		putItemInput := &dynamodb.PutItemInput{
			Item: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberN{Value: "2025"},
				"title": &types.AttributeValueMemberS{Value: fmt.Sprintf("Hello World %d", i)},
			},
			TableName: aws.String("movie"),
		}
		items = append(items, putItemInput.Item)

		_, err := ddb.PutItem(context.Background(), putItemInput)
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
	_, err = putItem(ddb)
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
		ConditionExpression: aws.String("attribute_not_exists(#regionCode) AND contains(#message, :message)"),
		ExpressionAttributeNames: map[string]string{
			"#regionCode": "regionCode",
			"#message":    "message",
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

func TestDeleteWithCondition(t *testing.T) {
	shutdown := startServer()
	defer shutdown()
	ddb := newDdbClient()
	_, err := createTable(ddb, 5, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Insert an item
	_, err = putItem(ddb)
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
	_, err = putItem(ddb)
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

func TestProvisionedThroughputExceededException(t *testing.T) {
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

func putItem(client *dynamodb.Client) (*dynamodb.PutItemOutput, error) {
	putItemInput := &dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			"year":    &types.AttributeValueMemberN{Value: "2025"},
			"title":   &types.AttributeValueMemberS{Value: "Hello World"},
			"message": &types.AttributeValueMemberS{Value: "your magic is mine"},
		},
		TableName: aws.String("movie"),
	}

	return client.PutItem(context.Background(), putItemInput)
}

func createTable(client *dynamodb.Client, readCapacity int64, writeCapacity int64) (*dynamodb.CreateTableOutput, error) {
	createTableInput := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("year"),
			AttributeType: types.ScalarAttributeTypeN,
		}, {
			AttributeName: aws.String("title"),
			AttributeType: types.ScalarAttributeTypeS,
		}, {
			AttributeName: aws.String("regionCode"),
			AttributeType: types.ScalarAttributeTypeS,
		}, {
			AttributeName: aws.String("countryCode"),
			AttributeType: types.ScalarAttributeTypeS,
		},
		},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("year"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("title"),
			KeyType:       types.KeyTypeRange,
		}},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String("regionGSI"),
			KeySchema: []types.KeySchemaElement{{
				AttributeName: aws.String("regionCode"),
				KeyType:       types.KeyTypeHash,
			}, {
				AttributeName: aws.String("countryCode"),
				KeyType:       types.KeyTypeRange,
			},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			// TODO: add check
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(readCapacity),
				WriteCapacityUnits: aws.Int64(writeCapacity),
			},
		}},
		TableName: aws.String("movie"),

		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(readCapacity),
			WriteCapacityUnits: aws.Int64(writeCapacity),
		},
	}
	output, err := client.CreateTable(context.TODO(), createTableInput)
	if err != nil {
		return nil, err
	}
	updateTestTableMetadata(client, 60, 60, 0)

	return output, nil
}

func updateTestTableMetadata(client *dynamodb.Client, tableDelaySeconds int, gsiDelaySeconds int, unprocessedRequests uint32) {
	_, err := client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			"tableName":           &types.AttributeValueMemberS{Value: "movie"},
			"tableDelaySeconds":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", tableDelaySeconds)},
			"gsiDelaySeconds":     &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", gsiDelaySeconds)},
			"unprocessedRequests": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", unprocessedRequests)},
		},
		TableName: aws.String("baddb_table_metadata"),
	})
	if err != nil {
		panic(err)
	}
}

func newDdbClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Using the Config value, create the DynamoDB client
	client := dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String("http://localhost:8080")
	})

	return client
}

func startServer() func() {
	svr := NewDdbServer()
	mux := http.NewServeMux()
	mux.HandleFunc("/", svr.Handler)

	port := 8080
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	log.Printf("baddb server is running on port %d...", port)

	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("Server error: %v\n", err)
		}

	}()

	return func() {
		err := server.Shutdown(context.Background())
		if err != nil {
			log.Printf("Server error: %v\n", err)
		}
	}
}
