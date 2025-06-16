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

	attrs, err := core.TransformAttributeValueMap(m)
	if err != nil {
		t.Fatalf("error transforming attributes: %v", err)
	}

	if *attrs["hashKey"].S != "hashKey1" {
		t.Fatalf("hashKey is not equal, expected :%s, got %s", "hashKey1", *attrs["hashKey"].S)
	}
	if *attrs["rangeKey"].S != "rangeKey1" {
		t.Fatalf("rangeKey is not equal, expected :%s, got %s", "rangeKey1", *attrs["rangeKey"].S)
	}
	if *attrs["count"].N != "9527" {
		t.Fatalf("count is not equal, expected :%s, got %s", "9527", *attrs["count"].N)
	}
	if attrs["ramens"].M == nil || *(*attrs["ramens"].M)["shio"].S != "Honmaru Tei" || *(*attrs["ramens"].M)["shoyu"].S != "Shigure" {
		t.Fatalf("ramens is not equal!")
	}

	_, err = core.EncodingAttributeValue(attrs)
	if err != nil {
		t.Fatal(err)
	}

}
