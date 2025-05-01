package server

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"strings"
	"testing"
)

func TestCreateTable(t *testing.T) {
	// TestCreateTable tests CreateTable function
	ddbLocal := newDdbLocalClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()
	defer shutdown()

	baddb := newBaddbClient()

	// Create table
	ddbOutput, ddbErr := createTable(ddbLocal)
	baddbOutput, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		if ddbErr != nil {
			t.Fatalf("failed to create table from ddb-local, %v", ddbErr)
		} else {
			t.Fatalf("failed to create table from baddb, %v", baddbErr)
		}
	}

	compareTableDescription(ddbOutput.TableDescription, baddbOutput.TableDescription)
}

func compareTableDescription(ddbTableDescription *types.TableDescription, baddbTableDescription *types.TableDescription) {
	if *ddbTableDescription.TableName != *baddbTableDescription.TableName {
		log.Fatalf("TableName is different, ddb has %s but baddb has %s", *ddbTableDescription.TableName, *baddbTableDescription.TableName)
	}

	if ddbTableDescription.TableStatus != baddbTableDescription.TableStatus {
		log.Fatalf("TableStatus is different, ddb has %s but baddb has %s", ddbTableDescription.TableStatus, baddbTableDescription.TableStatus)
	}

	if ddbTableDescription.ItemCount == nil && baddbTableDescription.ItemCount != nil {
		// no op
	} else if ddbTableDescription.ItemCount == nil || baddbTableDescription.ItemCount == nil {
		if ddbTableDescription.ItemCount == nil {
			log.Fatalf("ddb has nil ItemCount but baddb has %d", *baddbTableDescription.ItemCount)
		} else {
			log.Fatalf("ddb has %d but baddb has nil ItemCount", *ddbTableDescription.ItemCount)
		}

	} else if *ddbTableDescription.ItemCount != *baddbTableDescription.ItemCount {
		log.Fatalf("ItemCount is different, ddb has %d but baddb has %d", *ddbTableDescription.ItemCount, *baddbTableDescription.ItemCount)
	}

	if ddbTableDescription.TableSizeBytes == nil && baddbTableDescription.TableSizeBytes == nil {
		// no op
	} else if ddbTableDescription.TableSizeBytes == nil || baddbTableDescription.TableSizeBytes == nil {
		if ddbTableDescription.TableSizeBytes == nil {
			log.Fatalf("ddb has nil TableSizeBytes but baddb has %d", *baddbTableDescription.TableSizeBytes)
		} else {
			log.Fatalf("ddb has %d but baddb has nil TableSizeBytes", *ddbTableDescription.TableSizeBytes)
		}
	} else {
		// skip compare tableSizeBytes, it's difficult to match, and the value is not significant for now
	}

	if len(ddbTableDescription.AttributeDefinitions) != len(baddbTableDescription.AttributeDefinitions) {
		log.Fatalf("AttributeDefinitions are different, ddb has %d but baddb has %d", len(ddbTableDescription.AttributeDefinitions), len(baddbTableDescription.AttributeDefinitions))
	}
	for _, expectedDef := range ddbTableDescription.AttributeDefinitions {
		found := false
		for _, actualDef := range baddbTableDescription.AttributeDefinitions {
			if *expectedDef.AttributeName == *actualDef.AttributeName {
				found = true
				if expectedDef.AttributeType != actualDef.AttributeType {
					log.Fatalf("AttributeDefinitions are different, %v not match in baddb", *expectedDef.AttributeName)
				}
				break
			}
		}
		if !found {
			log.Fatalf("AttributeDefinitions are different, %v not found in baddb", *expectedDef.AttributeName)
		}
	}

	if len(ddbTableDescription.KeySchema) != len(baddbTableDescription.KeySchema) {
		log.Fatalf("KeySchema is different, ddb has %d but baddb has %d", len(ddbTableDescription.KeySchema), len(baddbTableDescription.KeySchema))
	}
	for i, expectedKeySchema := range ddbTableDescription.KeySchema {
		actualKeySchema := baddbTableDescription.KeySchema[i]
		if *expectedKeySchema.AttributeName != *actualKeySchema.AttributeName || expectedKeySchema.KeyType != actualKeySchema.KeyType {
			log.Fatalf("KeySchema is different at index %d, ddb has %v but baddb has %v", i, expectedKeySchema, actualKeySchema)
		}
	}

	if len(ddbTableDescription.GlobalSecondaryIndexes) != len(baddbTableDescription.GlobalSecondaryIndexes) {
		log.Fatalf("GlobalSecondaryIndexes are different, ddb has %d but baddb has %d", len(ddbTableDescription.GlobalSecondaryIndexes), len(baddbTableDescription.GlobalSecondaryIndexes))
	}
	for i, expectedGSI := range ddbTableDescription.GlobalSecondaryIndexes {
		actualGSI := baddbTableDescription.GlobalSecondaryIndexes[i]
		if *expectedGSI.IndexName != *actualGSI.IndexName {
			log.Fatalf("GlobalSecondaryIndexes are different at index %d, ddb has %s but baddb has %s", i, *expectedGSI.IndexName, *actualGSI.IndexName)
		}
		if len(expectedGSI.KeySchema) != len(actualGSI.KeySchema) {
			log.Fatalf("GlobalSecondaryIndexes KeySchema are different at index %d, ddb has %d but baddb has %d", i, len(expectedGSI.KeySchema), len(actualGSI.KeySchema))
		}
		for j, expectedKeySchema := range expectedGSI.KeySchema {
			actualKeySchema := actualGSI.KeySchema[j]
			if *expectedKeySchema.AttributeName != *actualKeySchema.AttributeName || expectedKeySchema.KeyType != actualKeySchema.KeyType {
				log.Fatalf("GlobalSecondaryIndexes KeySchema is different at index %d, ddb has %v but baddb has %v", j, expectedKeySchema, actualKeySchema)
			}
		}
		if expectedGSI.Projection.ProjectionType != actualGSI.Projection.ProjectionType {
			log.Fatalf("GlobalSecondaryIndexes ProjectionType is different at index %d, ddb has %s but baddb has %s", i, expectedGSI.Projection.ProjectionType, actualGSI.Projection.ProjectionType)
		}
	}

	if ddbTableDescription.ProvisionedThroughput == nil && baddbTableDescription.ProvisionedThroughput == nil {
		// no op
	} else if ddbTableDescription.ProvisionedThroughput == nil || baddbTableDescription.ProvisionedThroughput == nil {
		if ddbTableDescription.ProvisionedThroughput == nil {
			log.Fatalf("ddb has nil ProvisionedThroughput but baddb has %v", baddbTableDescription.ProvisionedThroughput)
		} else {
			log.Fatalf("ddb has %v but baddb has nil ProvisionedThroughput", ddbTableDescription.ProvisionedThroughput)
		}

	} else {
		if *ddbTableDescription.ProvisionedThroughput.ReadCapacityUnits != *baddbTableDescription.ProvisionedThroughput.ReadCapacityUnits {
			log.Fatalf("ProvisionedThroughput readCapacityUnits is different, ddb has %d but baddb has %d", *ddbTableDescription.ProvisionedThroughput.ReadCapacityUnits, *baddbTableDescription.ProvisionedThroughput.ReadCapacityUnits)
		}

		if *ddbTableDescription.ProvisionedThroughput.WriteCapacityUnits != *baddbTableDescription.ProvisionedThroughput.WriteCapacityUnits {
			log.Fatalf("ProvisionedThroughput writeCapacityUnits is different, ddb has %d but baddb has %d", *ddbTableDescription.ProvisionedThroughput.WriteCapacityUnits, *baddbTableDescription.ProvisionedThroughput.WriteCapacityUnits)
		}
	}
}

func cleanDdbLocal(client *dynamodb.Client) {
	// Clean up the table
	_, err := client.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: aws.String("movie"),
	})
	if err != nil && !strings.Contains(err.Error(), "Cannot do operations on a non-existent table") {
		log.Fatalf("failed to delete table from ddb-local, %v", err)
	}

}

func newBaddbClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Using the Config value, create the DynamoDB client
	client := dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String("http://localhost:8080")
	})

	return client
}

func newDdbLocalClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Using the Config value, create the DynamoDB client
	client := dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String("http://localhost:8000")
	})

	return client
}
