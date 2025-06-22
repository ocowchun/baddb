package integration

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strings"
	"testing"
)

func TestBatchWriteItemBehavior(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	tests := []struct {
		name          string
		existingItems []map[string]types.AttributeValue
		writeRequests []types.WriteRequest
	}{
		{
			name:          "batch put new items",
			existingItems: []map[string]types.AttributeValue{}, // no pre-existing items
			writeRequests: []types.WriteRequest{
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"year":     &types.AttributeValueMemberN{Value: "2024"},
							"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
							"language": &types.AttributeValueMemberS{Value: "English"},
						},
					},
				},
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"year":     &types.AttributeValueMemberN{Value: "2001"},
							"title":    &types.AttributeValueMemberS{Value: "The Lord of the Rings"},
							"language": &types.AttributeValueMemberS{Value: "English"},
						},
					},
				},
			},
		},
		{
			name: "batch delete existing items",
			existingItems: []map[string]types.AttributeValue{
				{
					"year":     &types.AttributeValueMemberN{Value: "1994"},
					"title":    &types.AttributeValueMemberS{Value: "Forrest Gump"},
					"language": &types.AttributeValueMemberS{Value: "English"},
				},
				{
					"year":     &types.AttributeValueMemberN{Value: "1999"},
					"title":    &types.AttributeValueMemberS{Value: "The Matrix"},
					"language": &types.AttributeValueMemberS{Value: "English"},
				},
			},
			writeRequests: []types.WriteRequest{
				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"year":  &types.AttributeValueMemberN{Value: "1994"},
							"title": &types.AttributeValueMemberS{Value: "Forrest Gump"},
						},
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"year":  &types.AttributeValueMemberN{Value: "1999"},
							"title": &types.AttributeValueMemberS{Value: "The Matrix"},
						},
					},
				},
			},
		},
		{
			name: "mixed put and delete operations",
			existingItems: []map[string]types.AttributeValue{
				{
					"year":     &types.AttributeValueMemberN{Value: "2010"},
					"title":    &types.AttributeValueMemberS{Value: "Inception"},
					"language": &types.AttributeValueMemberS{Value: "English"},
				},
			},
			writeRequests: []types.WriteRequest{
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"year":     &types.AttributeValueMemberN{Value: "2020"},
							"title":    &types.AttributeValueMemberS{Value: "Tenet"},
							"language": &types.AttributeValueMemberS{Value: "English"},
						},
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"year":  &types.AttributeValueMemberN{Value: "2010"},
							"title": &types.AttributeValueMemberS{Value: "Inception"},
						},
					},
				},
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"year":     &types.AttributeValueMemberN{Value: "2017"},
							"title":    &types.AttributeValueMemberS{Value: "Dunkirk"},
							"language": &types.AttributeValueMemberS{Value: "English"},
						},
					},
				},
			},
		},
		{
			name: "overwrite existing items",
			existingItems: []map[string]types.AttributeValue{
				{
					"year":     &types.AttributeValueMemberN{Value: "2008"},
					"title":    &types.AttributeValueMemberS{Value: "The Dark Knight"},
					"language": &types.AttributeValueMemberS{Value: "English"},
					"rating":   &types.AttributeValueMemberN{Value: "8.0"},
				},
			},
			writeRequests: []types.WriteRequest{
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"year":     &types.AttributeValueMemberN{Value: "2008"},
							"title":    &types.AttributeValueMemberS{Value: "The Dark Knight"},
							"language": &types.AttributeValueMemberS{Value: "English"},
							"rating":   &types.AttributeValueMemberN{Value: "9.0"},               // updated rating
							"director": &types.AttributeValueMemberS{Value: "Christopher Nolan"}, // new field
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pre-populate existing items
			for _, item := range tt.existingItems {
				putInput := &dynamodb.PutItemInput{
					TableName: aws.String("movie"),
					Item:      item,
				}
				_, err := ddbLocal.PutItem(context.TODO(), putInput)
				if err != nil {
					t.Fatalf("Failed to pre-populate ddbLocal: %v", err)
				}
				_, err = baddb.PutItem(context.TODO(), putInput)
				if err != nil {
					t.Fatalf("Failed to pre-populate baddb: %v", err)
				}
			}

			// Execute batch write
			ddbOut, ddbErr := batchWriteItem(ddbLocal, tt.writeRequests)
			baddbOut, baddbErr := batchWriteItem(baddb, tt.writeRequests)

			// Compare results
			if ddbErr != nil || baddbErr != nil {
				t.Fatalf("Unexpected errors: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}

			compareBatchWriteOutput(ddbOut, baddbOut, t)

			// Verify the operations actually worked by checking the final state
			verifyBatchWriteResults(ddbLocal, baddb, tt.writeRequests, t)
		})
	}
}

func TestBatchWriteItem_TooManyRequests(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	// Create more than 25 write requests (the limit)
	var writeRequests []types.WriteRequest
	for i := 0; i < 26; i++ {
		item := map[string]types.AttributeValue{
			"year":     &types.AttributeValueMemberN{Value: "2024"},
			"title":    &types.AttributeValueMemberS{Value: fmt.Sprintf("Movie-%d", i)},
			"language": &types.AttributeValueMemberS{Value: "English"},
		}
		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}

	ddbOut, ddbErr := batchWriteItem(ddbLocal, writeRequests)
	baddbOut, baddbErr := batchWriteItem(baddb, writeRequests)

	// Both should return validation errors
	if ddbOut != nil || baddbOut != nil {
		t.Fatalf("Expected nil outputs for too many requests, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}

	if ddbErr == nil || baddbErr == nil {
		t.Fatalf("Expected errors for too many requests, got ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	expectedErrorMessage := "Too many items requested for the BatchWriteItem call"
	if !strings.Contains(ddbErr.Error(), expectedErrorMessage) && !strings.Contains(ddbErr.Error(), "Member must have length less than or equal to") {
		t.Fatalf("Unexpected ddbErr message: %v", ddbErr)
	}
	if !strings.Contains(baddbErr.Error(), expectedErrorMessage) {
		t.Fatalf("Unexpected baddbErr message: %v", baddbErr)
	}
}

func TestBatchWriteItem_NonExistentTable(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	writeRequests := []types.WriteRequest{
		{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"year":  &types.AttributeValueMemberN{Value: "2024"},
					"title": &types.AttributeValueMemberS{Value: "Test Movie"},
				},
			},
		},
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"nonexistent_table": writeRequests,
		},
	}

	ddbOut, ddbErr := ddbLocal.BatchWriteItem(context.TODO(), input)
	baddbOut, baddbErr := baddb.BatchWriteItem(context.TODO(), input)

	if ddbOut != nil || baddbOut != nil {
		t.Fatalf("Expected nil outputs for non-existent table, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}

	if ddbErr == nil || baddbErr == nil {
		t.Fatalf("Expected errors for non-existent table, got ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
		t.Fatalf("BatchWriteItem errors differ: ddbErr=%s, baddbErr=%s", ddbErr.Error(), baddbErr.Error())
	}
}

func TestBatchWriteItem_EmptyRequest(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	// Empty write requests
	writeRequests := []types.WriteRequest{}

	ddbOut, ddbErr := batchWriteItem(ddbLocal, writeRequests)
	baddbOut, baddbErr := batchWriteItem(baddb, writeRequests)

	if ddbOut != nil || baddbOut != nil {
		t.Fatalf("Expected nil outputs for empty requests, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}

	if ddbErr == nil || baddbErr == nil {
		t.Fatalf("Expected errors for empty request, got ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
		t.Fatalf("BatchWriteItem errors differ: ddbErr=%s, baddbErr=%s", ddbErr.Error(), baddbErr.Error())
	}
}

func batchWriteItem(client *dynamodb.Client, writeRequests []types.WriteRequest) (*dynamodb.BatchWriteItemOutput, error) {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"movie": writeRequests,
		},
	}
	return client.BatchWriteItem(context.TODO(), input)
}

func compareBatchWriteOutput(ddbOut, baddbOut *dynamodb.BatchWriteItemOutput, t *testing.T) {
	if ddbOut == nil && baddbOut == nil {
		return
	}
	if ddbOut == nil || baddbOut == nil {
		t.Fatalf("Output mismatch: ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
		return
	}

	// Compare unprocessed items count
	ddbUnprocessedCount := len(ddbOut.UnprocessedItems)
	baddbUnprocessedCount := len(baddbOut.UnprocessedItems)

	if ddbUnprocessedCount != baddbUnprocessedCount {
		t.Fatalf("Unprocessed items count mismatch: ddbLocal=%d, baddb=%d", ddbUnprocessedCount, baddbUnprocessedCount)
	}

	// For successful operations, both should have no unprocessed items
	if ddbUnprocessedCount == 0 && baddbUnprocessedCount == 0 {
		return
	}

	// If there are unprocessed items, compare them
	for tableName, ddbUnprocessed := range ddbOut.UnprocessedItems {
		baddbUnprocessed, ok := baddbOut.UnprocessedItems[tableName]
		if !ok {
			t.Fatalf("Table %s has unprocessed items in ddbLocal but not in baddb", tableName)
			continue
		}
		if len(ddbUnprocessed) != len(baddbUnprocessed) {
			t.Fatalf("Unprocessed items count for table %s mismatch: ddbLocal=%d, baddb=%d", tableName, len(ddbUnprocessed), len(baddbUnprocessed))
		}
	}
}

func verifyBatchWriteResults(ddbLocal, baddb *dynamodb.Client, writeRequests []types.WriteRequest, t *testing.T) {
	for _, req := range writeRequests {
		var key map[string]types.AttributeValue
		var expectExists bool

		if req.PutRequest != nil {
			// For put requests, extract the key and expect the item to exist
			item := req.PutRequest.Item
			key = map[string]types.AttributeValue{
				"year":  item["year"],
				"title": item["title"],
			}
			expectExists = true
		} else if req.DeleteRequest != nil {
			// For delete requests, use the key and expect the item NOT to exist
			key = req.DeleteRequest.Key
			expectExists = false
		} else {
			continue // Skip unknown request types
		}

		getInput := &dynamodb.GetItemInput{
			TableName: aws.String("movie"),
			Key:       key,
		}

		ddbItem, ddbErr := ddbLocal.GetItem(context.TODO(), getInput)
		baddbItem, baddbErr := baddb.GetItem(context.TODO(), getInput)

		if ddbErr != nil || baddbErr != nil {
			t.Fatalf("Error verifying item: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			continue
		}

		ddbExists := ddbItem.Item != nil
		baddbExists := baddbItem.Item != nil

		if ddbExists != expectExists {
			t.Fatalf("DDB item existence mismatch: expected=%v, got=%v", expectExists, ddbExists)
		}
		if baddbExists != expectExists {
			t.Fatalf("BadDB item existence mismatch: expected=%v, got=%v", expectExists, baddbExists)
		}

		// If both items exist, compare their content
		if ddbExists && baddbExists {
			compareItem(ddbItem.Item, baddbItem.Item, t)
		}
	}
}
