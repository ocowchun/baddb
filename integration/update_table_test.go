package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strings"
	"testing"
)

func TestUpdateTableProvisionedThroughput(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()
	defer shutdown()

	baddb := newBaddbClient()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		if ddbErr != nil {
			t.Fatalf("failed to create table from ddb-local, %v", ddbErr)
		} else {
			t.Fatalf("failed to create table from baddb, %v", baddbErr)
		}
	}

	tableName := TestTableName

	newReadCapacity := int64(10)
	newWriteCapacity := int64(15)

	updateInput := &dynamodb.UpdateTableInput{
		TableName: &tableName,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &newReadCapacity,
			WriteCapacityUnits: &newWriteCapacity,
		},
	}

	ddbUpdateOutput, ddbUpdateErr := ddbLocal.UpdateTable(context.TODO(), updateInput)
	baddbUpdateOutput, baddbUpdateErr := baddb.UpdateTable(context.TODO(), updateInput)

	if ddbUpdateErr != nil || baddbUpdateErr != nil {
		if ddbUpdateErr != nil {
			t.Fatalf("failed to update table from ddb-local, %v", ddbUpdateErr)
		} else {
			t.Fatalf("failed to update table from baddb, %v", baddbUpdateErr)
		}
	}

	compareTableDescription(ddbUpdateOutput.TableDescription, baddbUpdateOutput.TableDescription)

	if *ddbUpdateOutput.TableDescription.ProvisionedThroughput.ReadCapacityUnits != newReadCapacity {
		t.Fatalf("read capacity not updated correctly in ddb-local")
	}
	if *baddbUpdateOutput.TableDescription.ProvisionedThroughput.ReadCapacityUnits != newReadCapacity {
		t.Fatalf("read capacity not updated correctly in baddb")
	}
	if *ddbUpdateOutput.TableDescription.ProvisionedThroughput.WriteCapacityUnits != newWriteCapacity {
		t.Fatalf("write capacity not updated correctly in ddb-local")
	}
	if *baddbUpdateOutput.TableDescription.ProvisionedThroughput.WriteCapacityUnits != newWriteCapacity {
		t.Fatalf("write capacity not updated correctly in baddb")
	}
}

func TestUpdateTableBillingMode(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()
	defer shutdown()

	baddb := newBaddbClient()

	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		if ddbErr != nil {
			t.Fatalf("failed to create table from ddb-local, %v", ddbErr)
		} else {
			t.Fatalf("failed to create table from baddb, %v", baddbErr)
		}
	}

	tableName := TestTableName

	updateInput := &dynamodb.UpdateTableInput{
		TableName:   &tableName,
		BillingMode: types.BillingModePayPerRequest,
	}

	ddbUpdateOutput, ddbUpdateErr := ddbLocal.UpdateTable(context.TODO(), updateInput)
	baddbUpdateOutput, baddbUpdateErr := baddb.UpdateTable(context.TODO(), updateInput)

	if ddbUpdateErr != nil || baddbUpdateErr != nil {
		if ddbUpdateErr != nil {
			t.Fatalf("failed to update table from ddb-local, %v", ddbUpdateErr)
		} else {
			t.Fatalf("failed to update table from baddb, %v", baddbUpdateErr)
		}
	}

	compareTableDescription(ddbUpdateOutput.TableDescription, baddbUpdateOutput.TableDescription)
}

func TestUpdateTableNonExistentTable(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()
	defer shutdown()

	baddb := newBaddbClient()

	nonExistentTableName := "NonExistentTable"
	updateInput := &dynamodb.UpdateTableInput{
		TableName:   &nonExistentTableName,
		BillingMode: types.BillingModePayPerRequest,
	}

	_, ddbUpdateErr := ddbLocal.UpdateTable(context.TODO(), updateInput)
	_, baddbUpdateErr := baddb.UpdateTable(context.TODO(), updateInput)

	if ddbUpdateErr == nil {
		t.Fatalf("expected error when updating non-existent table in ddb-local")
	}
	if baddbUpdateErr == nil {
		t.Fatalf("expected error when updating non-existent table in baddb")
	}

	// Both should return ResourceNotFound errors
	if !strings.Contains(ddbUpdateErr.Error(), "ResourceNotFound") {
		t.Fatalf("expected ResourceNotFound error from ddb-local, got: %v", ddbUpdateErr)
	}
	if !strings.Contains(baddbUpdateErr.Error(), "ResourceNotFound") {
		t.Fatalf("expected ResourceNotFound error from baddb, got: %v", baddbUpdateErr)
	}
}