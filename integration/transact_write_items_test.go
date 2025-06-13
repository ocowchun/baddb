package integration

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"
)

func TestTransactWriteItemsBehavior(t *testing.T) {
	tests := []struct {
		name      string
		condition *string
	}{
		{
			name:      "normal transact put",
			condition: nil,
		},
		{
			name:      "conditional transact put success",
			condition: aws.String("attribute_not_exists(title)"),
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

			ddbOut, ddbErr := transactPutItem(ddbLocal, tt.condition)
			baddbOut, baddbErr := transactPutItem(baddb, tt.condition)

			if ddbErr != nil {
				t.Errorf("ddbLocal: expected no error, got %v", ddbErr)
			}
			if baddbErr != nil {
				t.Errorf("baddb: expected no error, got %v", baddbErr)
			}

			compareTransactWriteOutput(ddbOut, baddbOut, t)
		})

		shutdown()
	}
}

func TestTransactWriteItemsWithConditionFailure(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	_, _ = transactPutItem(ddbLocal, nil)
	_, _ = transactPutItem(baddb, nil)

	condition := aws.String("attribute_not_exists(title)")

	ddbOut, ddbErr := transactPutItem(ddbLocal, condition)
	baddbOut, baddbErr := transactPutItem(baddb, condition)

	if ddbOut != nil || baddbOut != nil {
		t.Errorf("expected nil outputs, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}

	var ddbErrTyped *types.TransactionCanceledException
	var baddbErrTyped *types.TransactionCanceledException
	if !errors.As(ddbErr, &ddbErrTyped) {
		t.Errorf("expected ddbErr to be of type TransactionCanceledException, got %T", ddbErr)
	}
	if !errors.As(baddbErr, &baddbErrTyped) {
		t.Errorf("expected baddbErr to be of type TransactionCanceledException, got %T", ddbErr)
	}

	if !compareWithoutRequestID(ddbErrTyped.Error(), baddbErrTyped.Error()) {
		t.Errorf("expected errors to match, ddbErr=%v, baddbErr=%v", ddbErrTyped, baddbErrTyped)
	}
	for i, reason := range ddbErrTyped.CancellationReasons {
		baddbReason := baddbErrTyped.CancellationReasons[i]
		if *reason.Message != *baddbReason.Message {
			t.Errorf("message mismatch at index %d: ddb has %v but baddb has %v", i, reason.Message, baddbReason.Message)
		}
		if *reason.Code != *baddbReason.Code {
			t.Errorf("code mismatch at index %d: ddb has %v but baddb has %v", i, reason.Code, baddbReason.Code)
		}
	}

	shutdown()
}

func transactPutItem(client *dynamodb.Client, condition *string) (*dynamodb.TransactWriteItemsOutput, error) {
	put := types.TransactWriteItem{
		Put: &types.Put{
			TableName: aws.String("movie"),
			Item: map[string]types.AttributeValue{
				"year":     &types.AttributeValueMemberN{Value: "2024"},
				"title":    &types.AttributeValueMemberS{Value: "The Shawshank Redemption"},
				"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
				"language": &types.AttributeValueMemberS{Value: "English"},
			},
		},
	}
	if condition != nil {
		put.Put.ConditionExpression = condition
	}
	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{put},
	}
	return client.TransactWriteItems(context.TODO(), input)
}

func compareTransactWriteOutput(ddbOut, baddbOut *dynamodb.TransactWriteItemsOutput, t *testing.T) {
	// TransactWriteItemsOutput has no fields to compare for Put, but you can check for non-nil output
	if ddbOut == nil || baddbOut == nil {
		t.Errorf("expected non-nil outputs, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}
}
