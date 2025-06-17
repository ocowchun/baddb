package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
)

func TestBatchGetItemBehavior(t *testing.T) {
	items := []map[string]types.AttributeValue{
		{
			"year":  &types.AttributeValueMemberN{Value: "2024"},
			"title": &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
		},
		{
			"year":  &types.AttributeValueMemberN{Value: "2001"},
			"title": &types.AttributeValueMemberS{Value: "The Lord of the Rings: The Fellowship of the Ring"},
		},
		{
			"year":  &types.AttributeValueMemberN{Value: "1994"},
			"title": &types.AttributeValueMemberS{Value: "Forrest Gump"},
		},
	}

	tests := []struct {
		name       string
		insertIdx  []int // indices of items to insert
		requestIdx []int // indices of items to request
	}{
		{
			name:       "get 3 items, all exist",
			insertIdx:  []int{0, 1, 2},
			requestIdx: []int{0, 1, 2},
		},
		{
			name:       "get 3 items, only 2 exist",
			insertIdx:  []int{0, 2},
			requestIdx: []int{0, 1, 2},
		},
		{
			name:       "get 3 items, none exist",
			insertIdx:  []int{},
			requestIdx: []int{0, 1, 2},
		},
	}

	for _, tt := range tests {
		testContext := setupTest(t)
		ddbLocal := testContext.ddbLocal
		baddb := testContext.baddb

		// Insert items as needed
		for _, idx := range tt.insertIdx {
			putItem := &dynamodb.PutItemInput{
				TableName: aws.String("movie"),
				Item:      items[idx],
			}
			_, err := ddbLocal.PutItem(context.TODO(), putItem)
			if err != nil {
				t.Fatalf("ddbLocal PutItem failed: %v", err)
			}
			_, err = baddb.PutItem(context.TODO(), putItem)
			if err != nil {
				t.Fatalf("baddb PutItem failed: %v", err)
			}
		}

		// Prepare keys to request
		var keys []map[string]types.AttributeValue
		for _, idx := range tt.requestIdx {
			keys = append(keys, items[idx])
		}

		t.Run(tt.name, func(t *testing.T) {
			defer testContext.shutdown()
			ddbOut, ddbErr := batchGetItem(ddbLocal, keys)
			baddbOut, baddbErr := batchGetItem(baddb, keys)

			if ddbErr != nil || baddbErr != nil {
				t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}

			ddbItems := ddbOut.Responses["movie"]
			baddbItems := baddbOut.Responses["movie"]

			compareBatchGetItems(ddbItems, baddbItems, t)
		})
	}
}

func TestBatchGetItem_NonExistentTable(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	// Prepare keys (values don't matter since table doesn't exist)
	keys := []map[string]types.AttributeValue{
		{
			"year":  &types.AttributeValueMemberN{Value: "2024"},
			"title": &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
		},
	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"nonexistent_table": {
				Keys: keys,
			},
		},
	}

	ddbOut, ddbErr := ddbLocal.BatchGetItem(context.TODO(), input)
	baddbOut, baddbErr := baddb.BatchGetItem(context.TODO(), input)

	if ddbOut != nil || baddbOut != nil {
		t.Fatalf("expected no items, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}
	if ddbErr == nil || baddbErr == nil {
		t.Errorf("expected error for non-existent table, got ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
		t.Fatalf("BatchGetItem errors differ: ddbErr=%s, baddbErr=%s", ddbErr.Error(), baddbErr.Error())
	}
}

func TestBatchGetItem_InvalidKey(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	tests := []struct {
		name string
		keys []map[string]types.AttributeValue
	}{
		{
			name: "invalid key name",
			keys: []map[string]types.AttributeValue{
				{
					"wrong-year": &types.AttributeValueMemberN{Value: "2024"},
					"title":      &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				},
			},
		},
		{
			name: "invalid key type",
			keys: []map[string]types.AttributeValue{
				{
					"year":  &types.AttributeValueMemberS{Value: "2024"},
					"title": &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddbOut, ddbErr := batchGetItem(ddbLocal, tt.keys)
			baddbOut, baddbErr := batchGetItem(baddb, tt.keys)

			if ddbOut != nil || baddbOut != nil {
				t.Fatalf("expected no items, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
			}
			if ddbErr == nil || baddbErr == nil {
				t.Errorf("expected error for non-existent table, got ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}
			if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
				t.Fatalf("BatchGetItem errors differ: ddbErr=%s, baddbErr=%s", ddbErr.Error(), baddbErr.Error())
			}

		})

	}
}

func batchGetItem(client *dynamodb.Client, keys []map[string]types.AttributeValue) (*dynamodb.BatchGetItemOutput, error) {
	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"movie": {
				Keys: keys,
			},
		},
	}
	return client.BatchGetItem(context.TODO(), input)
}

func compareBatchGetItems(ddbItems, baddbItems []map[string]types.AttributeValue, t *testing.T) {
	if len(ddbItems) != len(baddbItems) {
		t.Errorf("item count mismatch: ddbLocal=%d, baddb=%d", len(ddbItems), len(baddbItems))
		return
	}
	// Compare each item (order is not guaranteed, so use a map for comparison)
	ddbMap := make(map[string]map[string]types.AttributeValue)
	for _, item := range ddbItems {
		ddbMap[itemKey(item)] = item
	}
	for _, item := range baddbItems {
		if _, ok := ddbMap[itemKey(item)]; !ok {
			t.Errorf("item not found in ddbLocal: %v", item)
		}
	}
}

func itemKey(item map[string]types.AttributeValue) string {
	return item["year"].(*types.AttributeValueMemberN).Value + "#" + item["title"].(*types.AttributeValueMemberS).Value
}
