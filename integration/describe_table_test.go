package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"testing"
)

func TestDescribeTable_Basic(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	input := &dynamodb.DescribeTableInput{
		TableName: aws.String("movie"),
	}

	ddbOut, ddbErr := ddbLocal.DescribeTable(context.TODO(), input)
	baddbOut, baddbErr := baddb.DescribeTable(context.TODO(), input)

	if ddbErr != nil || baddbErr != nil {
		t.Errorf("Unexpected errors: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	// Compare basic table properties
	ddbTable := ddbOut.Table
	baddbTable := baddbOut.Table

	if *ddbTable.TableName != *baddbTable.TableName {
		t.Errorf("Table name mismatch: ddbLocal=%s, baddb=%s", *ddbTable.TableName, *baddbTable.TableName)
	}

	if ddbTable.TableStatus != baddbTable.TableStatus {
		t.Errorf("Table status mismatch: ddbLocal=%s, baddb=%s", ddbTable.TableStatus, baddbTable.TableStatus)
	}

	// Compare key schema count
	if len(ddbTable.KeySchema) != len(baddbTable.KeySchema) {
		t.Errorf("Key schema length mismatch: ddbLocal=%d, baddb=%d", len(ddbTable.KeySchema), len(baddbTable.KeySchema))
	}

	// Compare GSI count
	if len(ddbTable.GlobalSecondaryIndexes) != len(baddbTable.GlobalSecondaryIndexes) {
		t.Errorf("GSI count mismatch: ddbLocal=%d, baddb=%d", len(ddbTable.GlobalSecondaryIndexes), len(baddbTable.GlobalSecondaryIndexes))
	}
}

func TestDescribeTable_NonExistentTable(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	input := &dynamodb.DescribeTableInput{
		TableName: aws.String("nonexistent_table"),
	}

	ddbOut, ddbErr := ddbLocal.DescribeTable(context.TODO(), input)
	baddbOut, baddbErr := baddb.DescribeTable(context.TODO(), input)

	if ddbOut != nil || baddbOut != nil {
		t.Errorf("Expected nil outputs for non-existent table, got ddbOut=%v, baddbOut=%v", ddbOut, baddbOut)
	}

	if ddbErr == nil || baddbErr == nil {
		t.Errorf("Expected errors for non-existent table, got ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	if !compareWithoutRequestID(ddbErr.Error(), baddbErr.Error()) {
		t.Errorf("DescribeTable errors differ: ddbErr=%s, baddbErr=%s", ddbErr.Error(), baddbErr.Error())
	}
}