package integration

import (
	"testing"
)

func TestDeleteTable(t *testing.T) {
	// TestDeleteTable tests DeleteTable function
	ddbLocal := newDdbLocalClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()
	defer shutdown()

	baddb := newBaddbClient()

	// Create table first
	_, ddbErr := createTable(ddbLocal)
	_, baddbErr := createTable(baddb)
	if ddbErr != nil || baddbErr != nil {
		if ddbErr != nil {
			t.Fatalf("failed to create table from ddb-local, %v", ddbErr)
		} else {
			t.Fatalf("failed to create table from baddb, %v", baddbErr)
		}
	}

	// Delete table
	ddbDelOut, ddbDelErr := deleteTable(ddbLocal, TestTableName)
	baddbDelOut, baddbDelErr := deleteTable(baddb, TestTableName)
	if ddbDelErr != nil || baddbDelErr != nil {
		if ddbDelErr != nil {
			t.Fatalf("failed to delete table from ddb-local, %v", ddbDelErr)
		} else {
			t.Fatalf("failed to delete table from baddb, %v", baddbDelErr)
		}
	}

	// Optionally, compare the delete output table names
	if *ddbDelOut.TableDescription.TableName != *baddbDelOut.TableDescription.TableName {
		t.Fatalf("TableName is different after delete, ddb has %s but baddb has %s", *ddbDelOut.TableDescription.TableName, *baddbDelOut.TableDescription.TableName)
	}
}

func TestDeleteTableWhenTableNotExists(t *testing.T) {
	ddbLocal := newDdbLocalClient()
	cleanDdbLocal(ddbLocal)
	shutdown := startServer()
	defer shutdown()

	baddb := newBaddbClient()

	deleteTable(ddbLocal, TestTableName)
	deleteTable(baddb, TestTableName)
	// Delete table
	ddbDelOut, ddbDelErr := deleteTable(ddbLocal, TestTableName)
	baddbDelOut, baddbDelErr := deleteTable(baddb, TestTableName)

	if ddbDelErr == nil || baddbDelErr == nil {
		t.Errorf("expected error for missing table, got ddbErr=%v, baddbErr=%v", ddbDelErr, baddbDelErr)
	}
	if ddbDelOut != nil || baddbDelOut != nil {
		t.Errorf("expected no output, got ddbDelOut=%v, baddbDelOut=%v", ddbDelOut, baddbDelOut)
	}

	if !compareWithoutRequestID(ddbDelErr.Error(), baddbDelErr.Error()) {
		t.Errorf("expected errors to match, ddbErr=%v, baddbErr=%v", ddbDelErr, baddbDelErr)
	}
}
