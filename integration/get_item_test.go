package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
)

func TestGetItemBehavior(t *testing.T) {
	tests := []struct {
		name        string
		insertItem  bool
		key         map[string]types.AttributeValue
		expectFound bool
	}{
		{
			name:       "get existing item",
			insertItem: true,
			key: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberN{Value: "2024"},
				"title": &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
			},
			expectFound: true,
		},
		{
			name:       "get non-existent item",
			insertItem: false,
			key: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberN{Value: "1999"},
				"title": &types.AttributeValueMemberS{Value: "The Matrix"},
			},
			expectFound: false,
		},
	}

	for _, tt := range tests {
		ddbLocal := newDdbLocalClient()
		baddb := newBaddbClient()
		cleanDdbLocal(ddbLocal)
		shutdown := startServer()

		_, ddbErr := createTable(ddbLocal)
		_, baddbErr := createTable(baddb)
		if ddbErr != nil || baddbErr != nil {
			t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
		}

		if tt.insertItem {
			_, err := putItemWithCondition(ddbLocal, nil)
			if err != nil {
				t.Fatalf("failed to put item in ddbLocal: %v", err)
			}
			_, err = putItemWithCondition(baddb, nil)
			if err != nil {
				t.Fatalf("failed to put item in baddb: %v", err)
			}
		}

		t.Run(tt.name, func(t *testing.T) {
			ddbOut, ddbErr := getItem(ddbLocal, tt.key)
			baddbOut, baddbErr := getItem(baddb, tt.key)

			if ddbErr != nil || baddbErr != nil {
				t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}

			if tt.expectFound {
				compareGetItemOutput(ddbOut, baddbOut, t)
			} else {
				if len(ddbOut.Item) != 0 || len(baddbOut.Item) != 0 {
					t.Errorf("expected no item, got ddbLocal=%v, baddb=%v", ddbOut.Item, baddbOut.Item)
				}
			}
		})

		shutdown()
	}
}

func getItem(client *dynamodb.Client, key map[string]types.AttributeValue) (*dynamodb.GetItemOutput, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String("movie"),
		Key:       key,
	}
	return client.GetItem(context.TODO(), input)
}

func compareGetItemOutput(ddbOutput, baddbOutput *dynamodb.GetItemOutput, t *testing.T) {
	compareItem(ddbOutput.Item, baddbOutput.Item, t)
}
