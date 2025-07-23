package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strings"
	"testing"
)

func TestUpdateTableProvisionedThroughput(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	newReadCapacity := int64(10)
	newWriteCapacity := int64(15)

	updateInput := &dynamodb.UpdateTableInput{
		TableName: aws.String(TestTableName),
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
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	updateInput := &dynamodb.UpdateTableInput{
		TableName:   aws.String(TestTableName),
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
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

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

func TestUpdateTableGSICreate(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	// Create a new GSI with a different attribute (reusing existing "title" attribute)
	updateInput := &dynamodb.UpdateTableInput{
		TableName: aws.String(TestTableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("title"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{
				Create: &types.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String("testGSI"),
					KeySchema: []types.KeySchemaElement{
						{
							AttributeName: aws.String("title"),
							KeyType:       types.KeyTypeHash,
						},
					},
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
					ProvisionedThroughput: &types.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(5),
						WriteCapacityUnits: aws.Int64(5),
					},
				},
			},
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
}
