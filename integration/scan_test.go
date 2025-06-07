package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sort"
	"testing"
)

func scanTestItems() []map[string]types.AttributeValue {
	return []map[string]types.AttributeValue{
		{
			"year":     &types.AttributeValueMemberN{Value: "2024"},
			"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
		},
		{
			"year":     &types.AttributeValueMemberN{Value: "1994"},
			"title":    &types.AttributeValueMemberS{Value: "Pulp Fiction"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "8.9"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
		},
		{
			"year":     &types.AttributeValueMemberN{Value: "2001"},
			"title":    &types.AttributeValueMemberS{Value: "Spirited Away"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "8.6"}}},
			"language": &types.AttributeValueMemberS{Value: "Japanese"},
		},
		{
			"year":     &types.AttributeValueMemberN{Value: "2010"},
			"title":    &types.AttributeValueMemberS{Value: "Inception"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "8.8"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
		},
		{
			"year":     &types.AttributeValueMemberN{Value: "1999"},
			"title":    &types.AttributeValueMemberS{Value: "The Matrix"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "8.7"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
		},
		{
			"year":     &types.AttributeValueMemberN{Value: "2014"},
			"title":    &types.AttributeValueMemberS{Value: "Interstellar"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "8.6"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
		},
		{
			"year":     &types.AttributeValueMemberN{Value: "1994"},
			"title":    &types.AttributeValueMemberS{Value: "Forrest Gump"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "8.8"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
		},
	}
}

func TestScanBehavior(t *testing.T) {
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
		_, err := putItemRaw(ddbLocal, item)
		if err != nil {
			t.Fatalf("failed to put item in ddbLocal: %v", err)
		}
		_, err = putItemRaw(baddb, item)
		if err != nil {
			t.Fatalf("failed to put item in baddb: %v", err)
		}
	}

	baseScanInput := &dynamodb.ScanInput{
		TableName: aws.String("movie"),
		Limit:     aws.Int32(2), // Set a limit to test pagination
	}
	ddbItems, ddbErr := scanAllPages(ddbLocal, baseScanInput)
	baddbItems, baddbErr := scanAllPages(baddb, baseScanInput)

	if ddbErr != nil || baddbErr != nil {
		t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	compareScanOutput(ddbItems, baddbItems, t)

	shutdown()
}

func TestScanBehaviorWithFilter(t *testing.T) {
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
		_, err := putItemRaw(ddbLocal, item)
		if err != nil {
			t.Fatalf("failed to put item in ddbLocal: %v", err)
		}
		_, err = putItemRaw(baddb, item)
		if err != nil {
			t.Fatalf("failed to put item in baddb: %v", err)
		}
	}

	baseScanInput := &dynamodb.ScanInput{
		TableName:        aws.String("movie"),
		Limit:            aws.Int32(2),
		FilterExpression: aws.String("#lang = :lang"),
		ExpressionAttributeNames: map[string]string{
			"#lang": "language",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lang": &types.AttributeValueMemberS{Value: "English"},
		},
	}
	ddbItems, ddbErr := scanAllPages(ddbLocal, baseScanInput)
	baddbItems, baddbErr := scanAllPages(baddb, baseScanInput)

	if ddbErr != nil || baddbErr != nil {
		t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	compareScanOutput(ddbItems, baddbItems, t)

	shutdown()
}

func TestScanBehaviorWithReservedWord(t *testing.T) {
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
		_, err := putItemRaw(ddbLocal, item)
		if err != nil {
			t.Fatalf("failed to put item in ddbLocal: %v", err)
		}
		_, err = putItemRaw(baddb, item)
		if err != nil {
			t.Fatalf("failed to put item in baddb: %v", err)
		}
	}

	baseScanInput := &dynamodb.ScanInput{
		TableName:        aws.String("movie"),
		Limit:            aws.Int32(2),
		FilterExpression: aws.String("language = :lang"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lang": &types.AttributeValueMemberS{Value: "English"},
		},
	}
	_, ddbErr = scanAllPages(ddbLocal, baseScanInput)
	_, baddbErr = scanAllPages(baddb, baseScanInput)

	if ddbErr == nil || baddbErr == nil {
		t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
		t.Errorf("Scan errors differ: ddbErr=%s, baddbErr=%s", ddbErr.Error(), baddbErr.Error())
	}

	shutdown()
}

func TestScanBehaviorGSI(t *testing.T) {
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
		_, err := putItemRaw(ddbLocal, item)
		if err != nil {
			t.Fatalf("failed to put item in ddbLocal: %v", err)
		}
		_, err = putItemRaw(baddb, item)
		if err != nil {
			t.Fatalf("failed to put item in baddb: %v", err)
		}
	}

	// Scan the GSI
	baseScanInput := &dynamodb.ScanInput{
		TableName: aws.String("movie"),
		IndexName: aws.String("gsiLanguage"),
		Limit:     aws.Int32(2),
	}
	ddbItems, ddbErr := scanAllPages(ddbLocal, baseScanInput)
	baddbItems, baddbErr := scanAllPages(baddb, baseScanInput)

	if ddbErr != nil || baddbErr != nil {
		t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	compareScanOutput(ddbItems, baddbItems, t)
	shutdown()
}

func TestScanBehaviorWithSegments(t *testing.T) {
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
		_, err := putItemRaw(ddbLocal, item)
		if err != nil {
			t.Fatalf("failed to put item in ddbLocal: %v", err)
		}
		_, err = putItemRaw(baddb, item)
		if err != nil {
			t.Fatalf("failed to put item in baddb: %v", err)
		}
	}

	totalSegments := int32(2)
	collectItems := func(client *dynamodb.Client) []map[string]types.AttributeValue {
		var allItems []map[string]types.AttributeValue
		for segment := int32(0); segment < totalSegments; segment++ {
			input := &dynamodb.ScanInput{
				TableName:     aws.String("movie"),
				Segment:       &segment,
				TotalSegments: &totalSegments,
				Limit:         aws.Int32(2), // Set a limit to test pagination
			}
			items, err := scanAllPages(client, input)
			if err != nil {
				t.Fatalf("scan failed for segment %d: %v", segment, err)
			}
			allItems = append(allItems, items...)
		}
		return allItems
	}

	ddbItems := collectItems(ddbLocal)
	baddbItems := collectItems(baddb)

	compareScanOutput(ddbItems, baddbItems, t)
	shutdown()
}

// Helper to insert a raw item
func putItemRaw(client *dynamodb.Client, item map[string]types.AttributeValue) (*dynamodb.PutItemOutput, error) {
	input := &dynamodb.PutItemInput{
		TableName: aws.String("movie"),
		Item:      item,
	}
	return client.PutItem(context.TODO(), input)
}

// Scan all pages with a given limit and collect all items
func scanAllPages(client *dynamodb.Client, baseScanInput *dynamodb.ScanInput) ([]map[string]types.AttributeValue, error) {
	var allItems []map[string]types.AttributeValue
	var lastKey map[string]types.AttributeValue
	for {
		input := &dynamodb.ScanInput{
			TableName:                 baseScanInput.TableName,
			FilterExpression:          baseScanInput.FilterExpression,
			ExpressionAttributeNames:  baseScanInput.ExpressionAttributeNames,
			ExpressionAttributeValues: baseScanInput.ExpressionAttributeValues,
			Limit:                     baseScanInput.Limit,
		}
		if lastKey != nil {
			input.ExclusiveStartKey = lastKey
		}
		out, err := client.Scan(context.TODO(), input)
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

func scanTable(client *dynamodb.Client) (*dynamodb.ScanOutput, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String("movie"),
	}
	return client.Scan(context.TODO(), input)
}

func compareScanOutput(ddbItems, baddbItems []map[string]types.AttributeValue, t *testing.T) {
	if len(ddbItems) != len(baddbItems) {
		t.Errorf("Scan item count differ: ddbLocal=%d, baddb=%d", len(ddbItems), len(baddbItems))
		return
	}

	sortItems(ddbItems)
	sortItems(baddbItems)
	for i := range ddbItems {
		compareItem(ddbItems[i], baddbItems[i], t)
	}
}

func sortItems(items []map[string]types.AttributeValue) {
	sort.Slice(items, func(i, j int) bool {
		yearI := items[i]["year"].(*types.AttributeValueMemberN).Value
		yearJ := items[j]["year"].(*types.AttributeValueMemberN).Value
		if yearI != yearJ {
			return yearI < yearJ
		}
		titleI := items[i]["title"].(*types.AttributeValueMemberS).Value
		titleJ := items[j]["title"].(*types.AttributeValueMemberS).Value
		return titleI < titleJ
	})
}
