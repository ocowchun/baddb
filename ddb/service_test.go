package ddb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ocowchun/baddb/ddb/core"
	"testing"
)

func TestEncoding(t *testing.T) {
	m := make(map[string]types.AttributeValue)
	m["hashKey"] = &types.AttributeValueMemberS{Value: "hashKey1"}
	m["rangeKey"] = &types.AttributeValueMemberS{Value: "rangeKey1"}
	m["count"] = &types.AttributeValueMemberN{Value: "9527"}
	ramens := make(map[string]types.AttributeValue)
	ramens["shio"] = &types.AttributeValueMemberS{Value: "Honmaru Tei"}
	ramens["shoyu"] = &types.AttributeValueMemberS{Value: "Shigure"}
	m["ramens"] = &types.AttributeValueMemberM{Value: ramens}

	entry := core.NewEntryFromItem(m)

	if *entry.Body["hashKey"].S != "hashKey1" {
		t.Fatalf("hashKey is not equal, expected :%s, got %s", "hashKey1", *entry.Body["hashKey"].S)
	}
	if *entry.Body["rangeKey"].S != "rangeKey1" {
		t.Fatalf("rangeKey is not equal, expected :%s, got %s", "rangeKey1", *entry.Body["rangeKey"].S)
	}
	if *entry.Body["count"].N != "9527" {
		t.Fatalf("count is not equal, expected :%s, got %s", "9527", *entry.Body["count"].N)
	}
	if entry.Body["ramens"].M == nil || *(*entry.Body["ramens"].M)["shio"].S != "Honmaru Tei" || *(*entry.Body["ramens"].M)["shoyu"].S != "Shigure" {
		t.Fatalf("ramens is not equal!")
	}

	_, err := core.EncodingAttributeValue(entry.Body)
	if err != nil {
		t.Fatal(err)
	}

}
