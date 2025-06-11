package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
)

func TestQueryByPartitionKey(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	for _, item := range scanTestItems() {
		_, _ = putItemRaw(ddbLocal, item)
		_, _ = putItemRaw(baddb, item)
	}

	keyCond := "#year = :year"
	exprVals := map[string]types.AttributeValue{
		":year": &types.AttributeValueMemberN{Value: "1994"},
	}
	input := &dynamodb.QueryInput{
		TableName:              aws.String("movie"),
		KeyConditionExpression: aws.String(keyCond),
		ExpressionAttributeNames: map[string]string{
			"#year": "year",
		},
		ExpressionAttributeValues: exprVals,
	}
	ddbOut, ddbErr := queryAllPages(ddbLocal, input)
	baddbOut, baddbErr := queryAllPages(baddb, input)

	if ddbErr != nil || baddbErr != nil {
		t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	compareScanOutput(ddbOut, baddbOut, t)
	shutdown()
}

func TestQueryWithFilter(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	for _, item := range scanTestItems() {
		_, _ = putItemRaw(ddbLocal, item)
		_, _ = putItemRaw(baddb, item)
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String("movie"),
		KeyConditionExpression: aws.String("#year = :year"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":year": &types.AttributeValueMemberN{Value: "1994"},
			":lang": &types.AttributeValueMemberS{Value: "English"},
		},
		FilterExpression: aws.String("#lang = :lang"),
		ExpressionAttributeNames: map[string]string{
			"#year": "year",
			"#lang": "language",
		},
	}
	ddbOut, ddbErr := queryAllPages(ddbLocal, input)
	baddbOut, baddbErr := queryAllPages(baddb, input)

	if ddbErr != nil || baddbErr != nil {
		t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	compareScanOutput(ddbOut, baddbOut, t)
	shutdown()
}

func TestQueryWithReservedWord(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	for _, item := range scanTestItems() {
		_, _ = putItemRaw(ddbLocal, item)
		_, _ = putItemRaw(baddb, item)
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String("movie"),
		KeyConditionExpression: aws.String("year = :year"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":year": &types.AttributeValueMemberN{Value: "1994"},
		},
	}
	_, ddbErr = queryAllPages(ddbLocal, input)
	_, baddbErr = queryAllPages(baddb, input)

	if ddbErr == nil || baddbErr == nil {
		t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
		t.Errorf("Query errors differ: ddbErr=%s, baddbErr=%s", ddbErr.Error(), baddbErr.Error())
	}

	shutdown()
}

func TestQueryGSI(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	for _, item := range scanTestItems() {
		_, _ = putItemRaw(ddbLocal, item)
		_, _ = putItemRaw(baddb, item)
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String("movie"),
		IndexName:              aws.String("gsiLanguage"),
		KeyConditionExpression: aws.String("#lang = :lang"),
		ExpressionAttributeNames: map[string]string{
			"#lang": "language",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lang": &types.AttributeValueMemberS{Value: "English"},
		},
	}
	ddbOut, ddbErr := queryAllPages(ddbLocal, input)
	baddbOut, baddbErr := queryAllPages(baddb, input)

	if ddbErr != nil || baddbErr != nil {
		t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	compareScanOutput(ddbOut, baddbOut, t)
	shutdown()
}

func TestQueryPartitionKeyAndSortKey(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	for _, item := range scanTestItems() {
		_, _ = putItemRaw(ddbLocal, item)
		_, _ = putItemRaw(baddb, item)
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String("movie"),
		KeyConditionExpression: aws.String("#year = :year AND title = :title"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":year":  &types.AttributeValueMemberN{Value: "2024"},
			":title": &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
		},
		ExpressionAttributeNames: map[string]string{
			"#year": "year",
		},
	}
	ddbOut, ddbErr := queryAllPages(ddbLocal, input)
	baddbOut, baddbErr := queryAllPages(baddb, input)

	if ddbErr != nil || baddbErr != nil {
		t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	compareScanOutput(ddbOut, baddbOut, t)
	shutdown()
}

// Helper to query all pages and collect items
func queryAllPages(client *dynamodb.Client, baseInput *dynamodb.QueryInput) ([]map[string]types.AttributeValue, error) {
	var allItems []map[string]types.AttributeValue
	var lastKey map[string]types.AttributeValue
	for {
		input := &dynamodb.QueryInput{
			TableName:                 baseInput.TableName,
			IndexName:                 baseInput.IndexName,
			KeyConditionExpression:    baseInput.KeyConditionExpression,
			FilterExpression:          baseInput.FilterExpression,
			ExpressionAttributeNames:  baseInput.ExpressionAttributeNames,
			ExpressionAttributeValues: baseInput.ExpressionAttributeValues,
			Limit:                     baseInput.Limit,
		}
		if lastKey != nil {
			input.ExclusiveStartKey = lastKey
		}
		out, err := client.Query(context.TODO(), input)
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, out.Items...)
		if out.LastEvaluatedKey == nil || len(out.LastEvaluatedKey) == 0 {
			break
		}
		lastKey = out.LastEvaluatedKey
	}
	return allItems, nil
}
