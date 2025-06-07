package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
)

func TestPutItemBehavior(t *testing.T) {

	tests := []struct {
		name            string
		condition       *string
		expectErr       bool
		hasPreviousItem bool
	}{
		{
			name:            "normal insert",
			condition:       nil,
			expectErr:       false,
			hasPreviousItem: false,
		},
		{
			name:            "conditional insert success",
			condition:       aws.String("attribute_not_exists(title)"),
			expectErr:       false,
			hasPreviousItem: false,
		},
		{
			name:            "conditional insert fails",
			condition:       aws.String("attribute_not_exists(title)"),
			expectErr:       true,
			hasPreviousItem: true, // This means we insert an item first to make the condition fail
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

			ddbOut, ddbErr := putItemWithCondition(ddbLocal, tt.condition)
			baddbOut, baddbErr := putItemWithCondition(baddb, tt.condition)

			if (ddbErr != nil) != tt.expectErr {
				t.Errorf("ddbLocal: expected error=%v, got %v", tt.expectErr, ddbErr)
			}
			if (baddbErr != nil) != tt.expectErr {
				t.Errorf("baddb: expected error=%v, got %v", tt.expectErr, baddbErr)
			}

			// Compare error types if both errored
			if tt.expectErr && ddbErr != nil && baddbErr != nil {
				if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
					t.Errorf("expected errors to match, ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
				}
			}

			if !tt.expectErr {
				comparePutItemOutput(ddbOut, baddbOut, t)
			}
		})

		// Clean up for next test case
		shutdown()
	}
}

func putItemWithCondition(client *dynamodb.Client, condition *string) (*dynamodb.PutItemOutput, error) {
	input := &dynamodb.PutItemInput{
		TableName: aws.String("movie"),
		Item: map[string]types.AttributeValue{
			"year":     &types.AttributeValueMemberN{Value: "2024"},
			"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
			"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
			"language": &types.AttributeValueMemberS{Value: "English"},
		},
	}
	if condition != nil {
		input.ConditionExpression = condition
	}
	return client.PutItem(context.TODO(), input)
}

// comparePutItemOutput compares the Attributes maps of two PutItemOutput objects without using reflection.
func comparePutItemOutput(ddbOutput *dynamodb.PutItemOutput, baddbOutput *dynamodb.PutItemOutput, t *testing.T) {
	ddbAttrs := ddbOutput.Attributes
	baddbAttrs := baddbOutput.Attributes

	compareItem(ddbAttrs, baddbAttrs, t)
}
