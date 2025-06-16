package integration

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strings"
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
		testContext := setupTest(t)
		ddbLocal := testContext.ddbLocal
		baddb := testContext.baddb

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

		testContext.shutdown()
	}
}

type TestContext struct {
	ddbLocal *dynamodb.Client
	baddb    *dynamodb.Client
	shutdown func()
}

func setupTest(t *testing.T) *TestContext {
	ddbLocal := newDdbLocalClient()
	baddb := newBaddbClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		t.Fatalf("failed to create table: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	return &TestContext{
		ddbLocal: ddbLocal,
		baddb:    baddb,
		shutdown: shutdown,
	}

}

func TestTransactWriteItemsWithConditionFailure(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb

	_, err := transactPutItem(ddbLocal, nil)
	if err != nil {
		t.Fatalf("failed to put item in ddbLocal: %v", err)
	}
	_, err = transactPutItem(baddb, nil)
	if err != nil {
		t.Fatalf("failed to put item in baddb: %v", err)
	}

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

	testContext.shutdown()
}

func TestTransactWriteItemsWithTooManyActionRequests(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb

	_, err := transactPutItem(ddbLocal, nil)
	if err != nil {
		t.Fatalf("failed to put item in ddbLocal: %v", err)
	}
	_, err = transactPutItem(baddb, nil)
	if err != nil {
		t.Fatalf("failed to put item in baddb: %v", err)
	}

	transactItems := make([]types.TransactWriteItem, 0)
	for len(transactItems) <= 100 {
		put := types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String("movie"),
				Item: map[string]types.AttributeValue{
					"year":     &types.AttributeValueMemberN{Value: "2024"},
					"title":    &types.AttributeValueMemberS{Value: fmt.Sprintf("The Shawshank Redemption-%d", len(transactItems))},
					"info":     &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"rating": &types.AttributeValueMemberN{Value: "9.3"}}},
					"language": &types.AttributeValueMemberS{Value: "English"},
				},
			},
		}
		transactItems = append(transactItems, put)

	}

	ddbOut, ddbErr := transactWriteItems(ddbLocal, transactItems)
	baddbOut, baddbErr := transactWriteItems(baddb, transactItems)

	if ddbOut != nil || baddbOut != nil {
		t.Errorf("expected nil outputs, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}

	if ddbErr == nil || baddbErr == nil {
		t.Errorf("expected errors, got ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}
	expectedErrorMessage := "api error ValidationException: Member must have length less than or equal to "
	if !strings.Contains(ddbErr.Error(), expectedErrorMessage) {
		t.Errorf("unexpected ddbErr message, got %v", ddbErr)
	}
	if !strings.Contains(baddbErr.Error(), expectedErrorMessage) {
		t.Errorf("unexpected baddbErr message, got %v", baddbErr)
	}

	testContext.shutdown()
}

func transactWriteItems(client *dynamodb.Client, transactItems []types.TransactWriteItem) (*dynamodb.TransactWriteItemsOutput, error) {
	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}

	return client.TransactWriteItems(context.TODO(), input)
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

	return transactWriteItems(client, []types.TransactWriteItem{put})
}

func compareTransactWriteOutput(ddbOut, baddbOut *dynamodb.TransactWriteItemsOutput, t *testing.T) {
	// TransactWriteItemsOutput has no fields to compare for Put, but you can check for non-nil output
	if ddbOut == nil || baddbOut == nil {
		t.Errorf("expected non-nil outputs, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}
}
