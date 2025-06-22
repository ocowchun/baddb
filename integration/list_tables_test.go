package integration

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strings"
	"testing"
)

func TestListTables_Basic(t *testing.T) {
	testContext := setupTest(t)
	ddbLocal := testContext.ddbLocal
	baddb := testContext.baddb
	defer testContext.shutdown()

	// Create two additional tables
	additionalTables := []string{"users", "products"}
	for _, tableName := range additionalTables {
		_, err := deleteTable(ddbLocal, tableName)
		if err != nil && !strings.Contains(err.Error(), "ResourceNotFoundException") {
			t.Fatalf("Failed to delete table %s: %v", tableName, err)
		}
	}

	for _, tableName := range additionalTables {
		createTableInput := &dynamodb.CreateTableInput{
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       types.KeyTypeHash,
				},
			},
			TableName:   aws.String(tableName),
			BillingMode: types.BillingModePayPerRequest,
		}

		_, ddbErr := ddbLocal.CreateTable(context.TODO(), createTableInput)
		_, baddbErr := baddb.CreateTable(context.TODO(), createTableInput)

		if ddbErr != nil || baddbErr != nil {
			t.Fatalf("Failed to create table %s: ddbErr=%v, baddbErr=%v", tableName, ddbErr, baddbErr)
		}
	}

	ddbOut, ddbErr := ddbLocal.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	baddbOut, baddbErr := baddb.ListTables(context.TODO(), &dynamodb.ListTablesInput{})

	if ddbErr != nil || baddbErr != nil {
		t.Errorf("Unexpected errors: ddbErr=%v, baddbErr=%v", ddbErr, baddbErr)
	}

	// Compare table count (should be 3: movie + users + products)
	expectedCount := 3
	if len(ddbOut.TableNames) != expectedCount {
		t.Errorf("Expected %d tables in ddbLocal, got %d", expectedCount, len(ddbOut.TableNames))
	}
	if len(baddbOut.TableNames) != expectedCount+1 { // baddb has an additional table "baddb_table_metadata"
		t.Errorf("Expected %d tables in baddb, got %d", expectedCount, len(baddbOut.TableNames))
	}
	for _, tableName := range ddbOut.TableNames {
		found := false
		for _, baddbTableName := range baddbOut.TableNames {
			if tableName == baddbTableName {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Table %s not found in baddb", tableName)
		}
	}

}
