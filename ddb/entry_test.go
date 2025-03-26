package ddb

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"testing"
)

func TestEntrySet(t *testing.T) {
	m := map[string]AttributeValue{
		"key": {
			S: aws.String("value"),
		},
	}
	list := []AttributeValue{
		{
			S: aws.String("value1"),
		},
		{
			S: aws.String("value2"),
		},
	}
	entry := &Entry{
		Body: map[string]AttributeValue{
			"foo": {
				S: aws.String("bar"),
			},
			"map": {
				M: &m,
			},
			"list": {
				L: &list,
			},
		},
	}

	err := entry.Set(&AttributeNameOperand{Name: "foo"}, AttributeValue{S: aws.String("baz")})
	if err != nil {
		t.Fatal(err)
	}
	if *entry.Body["foo"].S != "baz" {
		t.Fatalf("expected %v, got %v", "baz", *entry.Body["foo"].S)
	}

	err = entry.Set(&IndexOperand{
		Left:  &AttributeNameOperand{Name: "list"},
		Index: 1,
	}, AttributeValue{S: aws.String("new value")})
	if err != nil {
		t.Fatal(err)
	}
	if *((*entry.Body["list"].L)[1].S) != "new value" {
		t.Fatalf("expected %v, got %v", "new value", *((*entry.Body["list"].L)[1].S))
	}

	err = entry.Set(&DotOperand{
		Left:  &AttributeNameOperand{Name: "map"},
		Right: &AttributeNameOperand{Name: "key"},
	}, AttributeValue{S: aws.String("new value")})
	if err != nil {
		t.Fatal(err)
	}
	if *((*entry.Body["map"].M)["key"].S) != "new value" {
		t.Fatalf("expected %v, got %v", "new value", *entry.Body["foo"].S)
	}
}
