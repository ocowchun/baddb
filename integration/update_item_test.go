package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
)

func TestUpdateItemBehavior(t *testing.T) {
	tests := []struct {
		name            string
		condition       *string
		expectErr       bool
		hasPreviousItem bool
	}{
		{
			name:            "normal update when item does not exist",
			condition:       nil,
			expectErr:       false,
			hasPreviousItem: false,
		},
		{
			name:            "normal update",
			condition:       nil,
			expectErr:       false,
			hasPreviousItem: true,
		},
		{
			name:            "conditional update success",
			condition:       aws.String("attribute_exists(title)"),
			expectErr:       false,
			hasPreviousItem: true,
		},
		{
			name:            "conditional update fails",
			condition:       aws.String("attribute_not_exists(title)"),
			expectErr:       true,
			hasPreviousItem: true,
		},
		{
			name:            "conditional update fails due to reserved keyword",
			condition:       aws.String("attribute_not_exists(language)"),
			expectErr:       true,
			hasPreviousItem: false,
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

		t.Run(tt.name, func(t *testing.T) {
			if tt.hasPreviousItem {
				_, _ = putItemWithCondition(ddbLocal, nil)
				_, _ = putItemWithCondition(baddb, nil)
			}

			ddbOut, ddbErr := updateItemWithCondition(ddbLocal, tt.condition)
			baddbOut, baddbErr := updateItemWithCondition(baddb, tt.condition)

			if (ddbErr != nil) != tt.expectErr {
				t.Errorf("ddbLocal: expected error=%v, got %v", tt.expectErr, ddbErr)
			}
			if (baddbErr != nil) != tt.expectErr {
				t.Errorf("baddb: expected error=%v, got %v", tt.expectErr, baddbErr)
			}

			if tt.expectErr && ddbErr != nil && baddbErr != nil {
				if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
					t.Errorf("expected errors to match, ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
				}
			}

			if !tt.expectErr {
				compareUpdateItemOutput(ddbOut, baddbOut, t)
			}
		})

		shutdown()
	}
}

func updateItemWithCondition(client *dynamodb.Client, condition *string) (*dynamodb.UpdateItemOutput, error) {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String("movie"),
		Key: map[string]types.AttributeValue{
			"year":  &types.AttributeValueMemberN{Value: "2024"},
			"title": &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
		},
		UpdateExpression:          aws.String("SET #L = :lang"),
		ExpressionAttributeNames:  map[string]string{"#L": "language"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":lang": &types.AttributeValueMemberS{Value: "French"}},
		ReturnValues:              types.ReturnValueAllNew,
	}
	if condition != nil {
		input.ConditionExpression = condition
	}
	return client.UpdateItem(context.TODO(), input)
}

func compareUpdateItemOutput(ddbOutput *dynamodb.UpdateItemOutput, baddbOutput *dynamodb.UpdateItemOutput, t *testing.T) {
	compareItem(ddbOutput.Attributes, baddbOutput.Attributes, t)
}
