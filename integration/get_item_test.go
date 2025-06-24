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
		key         map[string]types.AttributeValue
		expectFound bool
		existsItem  map[string]types.AttributeValue
	}{
		{
			name: "get existing item",
			key: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberN{Value: "2024"},
				"title": &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
			},
			expectFound: true,
			existsItem: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "2024"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
		},
		{
			name: "get non-existent item",
			key: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberN{Value: "1999"},
				"title": &types.AttributeValueMemberS{Value: "The Matrix"},
			},
			expectFound: false,
			existsItem:  nil,
		},
	}

	for _, tt := range tests {
		testContext := setupTest(t)
		ddbLocal := testContext.ddbLocal
		baddb := testContext.baddb

		if tt.existsItem != nil {
			input := &dynamodb.PutItemInput{
				TableName: aws.String(TestTableName),
				Item:      tt.existsItem,
			}
			_, err := putItem(ddbLocal, input)
			if err != nil {
				t.Fatalf("failed to request item in ddbLocal: %v", err)
			}
			_, err = putItem(baddb, input)
			if err != nil {
				t.Fatalf("failed to request item in baddb: %v", err)
			}
		}

		t.Run(tt.name, func(t *testing.T) {
			defer testContext.shutdown()
			ddbOut, ddbErr := getItem(ddbLocal, tt.key)
			baddbOut, baddbErr := getItem(baddb, tt.key)

			if ddbErr != nil || baddbErr != nil {
				t.Errorf("unexpected error: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}

			if tt.expectFound {
				compareGetItemOutput(ddbOut, baddbOut, t)
			} else {
				if ddbOut.Item != nil || baddbOut.Item != nil {
					t.Errorf("expected item to be nil, got ddbLocal=%v, baddb=%v", ddbOut.Item == nil, baddbOut.Item == nil)
				}

				if len(ddbOut.Item) != 0 || len(baddbOut.Item) != 0 {
					t.Errorf("expected no item, got ddbLocal=%v, baddb=%v", ddbOut.Item, baddbOut.Item)
				}
			}
		})
	}
}

func TestGetItem_TableNotExists(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	key := map[string]types.AttributeValue{
		"year":  &types.AttributeValueMemberN{Value: "2000"},
		"title": &types.AttributeValueMemberS{Value: "Gladiator"},
	}

	ddbOut, ddbErr := getItem(ddbLocal, key)
	baddbOut, baddbErr := getItem(baddb, key)

	if ddbErr == nil || baddbErr == nil {
		t.Errorf("expected error for missing table, got ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	if ddbErr != nil && baddbErr != nil && !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
		t.Errorf("expected errors to match, ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	if ddbOut != nil && len(ddbOut.Item) != 0 {
		t.Errorf("expected no item from ddbLocal, got %v", ddbOut.Item)
	}
	if baddbOut != nil && len(baddbOut.Item) != 0 {
		t.Errorf("expected no item from baddb, got %v", baddbOut.Item)
	}

	shutdown()
}

func TestGetItem_InvalidKey(t *testing.T) {
	tests := []struct {
		name string
		key  map[string]types.AttributeValue
	}{
		{
			name: "invalid key name",
			key: map[string]types.AttributeValue{
				"wrong-year": &types.AttributeValueMemberN{Value: "2000"},
				"title":      &types.AttributeValueMemberS{Value: "Gladiator"},
			},
		},
		{
			name: "invalid key type",
			key: map[string]types.AttributeValue{
				"year":  &types.AttributeValueMemberS{Value: "2000"},
				"title": &types.AttributeValueMemberS{Value: "Gladiator"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testContext := setupTest(t)
			ddbLocal := testContext.ddbLocal
			baddb := testContext.baddb
			defer testContext.shutdown()

			ddbOut, ddbErr := getItem(ddbLocal, tt.key)
			baddbOut, baddbErr := getItem(baddb, tt.key)

			if ddbErr == nil || baddbErr == nil {
				t.Errorf("expected error for missing table, got ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}
			if ddbErr != nil && baddbErr != nil && !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
				t.Errorf("expected errors to match, ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
			}
			if ddbOut != nil && len(ddbOut.Item) != 0 {
				t.Errorf("expected no item from ddbLocal, got %v", ddbOut.Item)
			}
			if baddbOut != nil && len(baddbOut.Item) != 0 {
				t.Errorf("expected no item from baddb, got %v", baddbOut.Item)
			}
		})
	}

	//  GetItem, https response error StatusCode: 400, RequestID: 5f750dc4-6e63-4d58-b062-9b761933aece, api error ValidationException: One of the required keys was not given a value, baddbErr=<nil>
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
