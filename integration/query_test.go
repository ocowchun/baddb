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

	for _, item := range queryTestItems() {
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
		t.Fatalf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	compareItems(ddbOut, baddbOut, t)
	shutdown()
}

func TestQueryByPartitionKeyWithInvalidInput(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	for _, item := range queryTestItems() {
		_, _ = putItemRaw(ddbLocal, item)
		_, _ = putItemRaw(baddb, item)
	}

	tests := []struct {
		name           string
		keyCond        string
		exprAttrNames  map[string]string
		exprAttrValues map[string]types.AttributeValue
	}{
		{
			name:    "invalid partition key type",
			keyCond: "#year = :year",
			exprAttrNames: map[string]string{
				"#year": "year",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year": &types.AttributeValueMemberS{Value: "1994"}, // Incorrect type, should be N
			},
		},
		{
			name:    "invalid partition key comparator",
			keyCond: "#year > :year",
			exprAttrNames: map[string]string{
				"#year": "year",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year": &types.AttributeValueMemberN{Value: "1994"},
			},
		},
		{
			name:    "missing partition key",
			keyCond: "#title = :title",
			exprAttrNames: map[string]string{
				"#title": "title",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":title": &types.AttributeValueMemberS{Value: "The Shawshank Redemption"}, // No partition key provided
			},
		},
		{
			name:    "invalid partition key value",
			keyCond: "#year = :year",
			exprAttrNames: map[string]string{
				"#year": "year",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year": &types.AttributeValueMemberN{Value: "言って"},
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &dynamodb.QueryInput{
				TableName:                 aws.String("movie"),
				KeyConditionExpression:    aws.String(tt.keyCond),
				ExpressionAttributeNames:  tt.exprAttrNames,
				ExpressionAttributeValues: tt.exprAttrValues,
			}
			ddbOut, ddbErr := queryAllPages(ddbLocal, input)
			baddbOut, baddbErr := queryAllPages(baddb, input)

			if ddbOut != nil || baddbOut != nil {
				t.Fatalf("expected no items, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
			}
			if ddbErr == nil || baddbErr == nil {
				t.Fatalf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}
			if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
				t.Fatalf("Query errors differ: ddbErr=%s, baddbErr=%s", ddbErr.Error(), baddbErr.Error())
			}
		})
	}

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

	for _, item := range queryTestItems() {
		_, _ = putItemRaw(ddbLocal, item)
		_, _ = putItemRaw(baddb, item)
	}

	tests := []struct {
		name             string
		filterExpression string
		exprAttrNames    map[string]string
		exprAttrValues   map[string]types.AttributeValue
	}{
		{
			name:             "filter by language",
			filterExpression: "#lang = :lang",
			exprAttrNames: map[string]string{
				"#year": "year",
				"#lang": "language",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year": &types.AttributeValueMemberN{Value: "2001"},
				":lang": &types.AttributeValueMemberS{Value: "Japanese"},
			},
		},

		{
			name:             "filter by rating = 8.6",
			filterExpression: "#info.#rating = :rating",
			exprAttrNames: map[string]string{
				"#year":   "year",
				"#info":   "info",
				"#rating": "rating",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year":   &types.AttributeValueMemberN{Value: "2001"},
				":rating": &types.AttributeValueMemberN{Value: "8.6"},
			},
		},

		{
			name:             "filter by rating <> 8.6",
			filterExpression: "#info.#rating <> :rating",
			exprAttrNames: map[string]string{
				"#year":   "year",
				"#info":   "info",
				"#rating": "rating",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year":   &types.AttributeValueMemberN{Value: "2001"},
				":rating": &types.AttributeValueMemberN{Value: "8.6"},
			},
		},

		{
			name:             "filter by rating > 8.6",
			filterExpression: "#info.#rating > :rating",
			exprAttrNames: map[string]string{
				"#year":   "year",
				"#info":   "info",
				"#rating": "rating",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year":   &types.AttributeValueMemberN{Value: "2001"},
				":rating": &types.AttributeValueMemberN{Value: "8.6"},
			},
		},

		{
			name:             "filter by rating >= 8.6",
			filterExpression: "#info.#rating > :rating",
			exprAttrNames: map[string]string{
				"#year":   "year",
				"#info":   "info",
				"#rating": "rating",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year":   &types.AttributeValueMemberN{Value: "2001"},
				":rating": &types.AttributeValueMemberN{Value: "8.6"},
			},
		},

		{
			name:             "filter by rating < 8.6",
			filterExpression: "#info.#rating > :rating",
			exprAttrNames: map[string]string{
				"#year":   "year",
				"#info":   "info",
				"#rating": "rating",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year":   &types.AttributeValueMemberN{Value: "2001"},
				":rating": &types.AttributeValueMemberN{Value: "8.6"},
			},
		},

		{
			name:             "filter by rating <= 8.6",
			filterExpression: "#info.#rating > :rating",
			exprAttrNames: map[string]string{
				"#year":   "year",
				"#info":   "info",
				"#rating": "rating",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year":   &types.AttributeValueMemberN{Value: "2001"},
				":rating": &types.AttributeValueMemberN{Value: "8.6"},
			},
		},

		{
			name:             "filter by language",
			filterExpression: "begins_with(#lang, :lang)",
			exprAttrNames: map[string]string{
				"#year": "year",
				"#lang": "language",
			},
			exprAttrValues: map[string]types.AttributeValue{
				":year": &types.AttributeValueMemberN{Value: "2001"},
				":lang": &types.AttributeValueMemberS{Value: "Eng"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &dynamodb.QueryInput{
				TableName:                 aws.String("movie"),
				KeyConditionExpression:    aws.String("#year = :year"),
				FilterExpression:          aws.String(tt.filterExpression),
				ExpressionAttributeNames:  tt.exprAttrNames,
				ExpressionAttributeValues: tt.exprAttrValues,
			}
			ddbOut, ddbErr := queryAllPages(ddbLocal, input)
			baddbOut, baddbErr := queryAllPages(baddb, input)

			if ddbErr != nil || baddbErr != nil {
				t.Fatalf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}
			compareItems(ddbOut, baddbOut, t)
		})
	}

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

	for _, item := range queryTestItems() {
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
	ddbOut, ddbErr := queryAllPages(ddbLocal, input)
	baddbOut, baddbErr := queryAllPages(baddb, input)

	if ddbOut != nil || baddbOut != nil {
		t.Fatalf("expected no items, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}

	if ddbErr == nil || baddbErr == nil {
		t.Fatalf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
		t.Fatalf("Query errors differ: ddbErr=%s, baddbErr=%s", ddbErr.Error(), baddbErr.Error())
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

	for _, item := range queryTestItems() {
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
		t.Fatalf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	compareItems(ddbOut, baddbOut, t)
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

	for _, item := range queryTestItems() {
		_, _ = putItemRaw(ddbLocal, item)
		_, _ = putItemRaw(baddb, item)
	}

	tests := []struct {
		name             string
		keyConditionExpr string
		exprAttrNames    map[string]string
		exprAttrValues   map[string]types.AttributeValue
	}{
		{
			name:             "partition and sort key equals",
			keyConditionExpr: "#year = :year AND title = :title",
			exprAttrNames:    map[string]string{"#year": "year"},
			exprAttrValues: map[string]types.AttributeValue{
				":year":  &types.AttributeValueMemberN{Value: "2024"},
				":title": &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
			},
		},
		{
			name:             "sort key begins_with",
			keyConditionExpr: "#year = :year AND begins_with(title, :prefix)",
			exprAttrNames:    map[string]string{"#year": "year"},
			exprAttrValues: map[string]types.AttributeValue{
				":year":   &types.AttributeValueMemberN{Value: "2001"},
				":prefix": &types.AttributeValueMemberS{Value: "The"},
			},
		},
		{
			name:             "sort key between",
			keyConditionExpr: "#year = :year AND title BETWEEN :start AND :end",
			exprAttrNames:    map[string]string{"#year": "year"},
			exprAttrValues: map[string]types.AttributeValue{
				":year":  &types.AttributeValueMemberN{Value: "1994"},
				":start": &types.AttributeValueMemberS{Value: "Forrest Gump"},
				":end":   &types.AttributeValueMemberS{Value: "Pulp Fiction"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := &dynamodb.QueryInput{
				TableName:                 aws.String("movie"),
				KeyConditionExpression:    aws.String(tc.keyConditionExpr),
				ExpressionAttributeNames:  tc.exprAttrNames,
				ExpressionAttributeValues: tc.exprAttrValues,
			}
			ddbOut, ddbErr := queryAllPages(ddbLocal, input)
			baddbOut, baddbErr := queryAllPages(baddb, input)

			if ddbErr != nil || baddbErr != nil {
				t.Fatalf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}
			compareItems(ddbOut, baddbOut, t)
		})
	}
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

func queryTestItems() []map[string]types.AttributeValue {
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
			"year":     &types.AttributeValueMemberN{Value: "1994"},
			"title":    &types.AttributeValueMemberS{Value: "The Lion King"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "8.5"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
		},
		{
			"year":     &types.AttributeValueMemberN{Value: "2001"},
			"title":    &types.AttributeValueMemberS{Value: "Spirited Away"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "8.6"}}},
			"language": &types.AttributeValueMemberS{Value: "Japanese"},
		},
		{
			"year":     &types.AttributeValueMemberN{Value: "2001"},
			"title":    &types.AttributeValueMemberS{Value: "The Lord of the Rings: The Fellowship of the Ring"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "8.8"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
		},
		{
			"year":     &types.AttributeValueMemberN{Value: "2001"},
			"title":    &types.AttributeValueMemberS{Value: "The Mummy Returns"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "6.4"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
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
