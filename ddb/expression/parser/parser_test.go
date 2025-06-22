package parser

import (
	"github.com/ocowchun/baddb/ddb/expression/lexer"
	"strings"
	"testing"
)

func TestParseKeyConditionExpression(t *testing.T) {
	keyConditionExpressions := []string{
		"partitionKeyName = :partitionkeyval",
		"partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval",
		"partitionKeyName = :partitionkeyval AND sortKeyName < :sortkeyval",
		"partitionKeyName = :partitionkeyval AND sortKeyName <= :sortkeyval",
		"partitionKeyName = :partitionkeyval AND sortKeyName > :sortkeyval",
		"partitionKeyName = :partitionkeyval AND sortKeyName >= :sortkeyval",
		"sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2",
		"begins_with(sortKeyName, :sortkeyval)",
		"#partitionKeyName = :partitionkeyval",
	}
	for _, content := range keyConditionExpressions {
		l := lexer.New(strings.NewReader(content))
		p := New(l)

		exp, err := p.ParseKeyConditionExpression()
		if err != nil {
			t.Fatal(err)
		}

		if exp.String() != content {
			t.Fatalf("expect %s but get %s", content, exp.String())
		}
	}
}

func TestParseOperand(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"attributeName", "attributeName"},
		{"#attributeName", "#attributeName"},
		{":attributeValue", ":attributeValue"},
		{"attributeName.subAttribute", "attributeName.subAttribute"},
		{"attributeName[0]", "attributeName[0]"},
		{":attributeName.subAttribute", ":attributeName.subAttribute"},
		{"ProductReviews.FiveStar[0]", "ProductReviews.FiveStar[0]"},
		{"size(attributeName)", "size(attributeName)"},
	}

	for _, tt := range tests {
		l := lexer.New(strings.NewReader(tt.input))
		p := New(l)

		operand, err := p.parseOperand()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if operand.String() != tt.expected {
			t.Fatalf("expected %s but got %s", tt.expected, operand.String())
		}
	}
}

func TestParseFunctionConditionExpression(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"attribute_exists(attributeName)", "attribute_exists(attributeName)"},
		{"attribute_not_exists(attributeName)", "attribute_not_exists(attributeName)"},
		{"attribute_type(attributeName, S)", "attribute_type(attributeName, S)"},
		{"begins_with(attributeName, prefix)", "begins_with(attributeName, prefix)"},
		{"contains(attributeName, operand)", "contains(attributeName, operand)"},
	}

	for _, tt := range tests {
		l := lexer.New(strings.NewReader(tt.input))
		p := New(l)

		functionExpr, err := p.parseFunctionConditionExpression()
		if err != nil {
			t.Fatalf("unexpected error: %v when parsing %s", err, tt.input)
		}

		if functionExpr.String() != tt.expected {
			t.Fatalf("expected %s but got %s", tt.expected, functionExpr.String())
		}
	}
}

func TestParseConditionExpression(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"attributeName = :attributeValue", "attributeName = :attributeValue"},
		{"attributeName <> :attributeValue", "attributeName <> :attributeValue"},
		{"attributeName < :attributeValue", "attributeName < :attributeValue"},
		{"attributeName <= :attributeValue", "attributeName <= :attributeValue"},
		{"attributeName > :attributeValue", "attributeName > :attributeValue"},
		{"attributeName >= :attributeValue", "attributeName >= :attributeValue"},
		{"attributeName BETWEEN :value1 AND :value2", "attributeName BETWEEN :value1 AND :value2"},
		{"attributeName IN (:value1, :value2, :value3)", "attributeName IN (:value1, :value2, :value3)"},
		{"attribute_exists(attributeName)", "attribute_exists(attributeName)"},
		{"NOT attribute_exists(attributeName)", "NOT attribute_exists(attributeName)"},
		{"(attributeName = :attributeValue)", "attributeName = :attributeValue"},
		{"attributeName = :attributeValue AND attributeName2 = :attributeValue2", "(attributeName = :attributeValue AND attributeName2 = :attributeValue2)"},
		{"attributeName = :attributeValue OR attributeName2 = :attributeValue2", "(attributeName = :attributeValue OR attributeName2 = :attributeValue2)"},
		{"attributeName = :attributeValue OR attributeName2 = :attributeValue2 AND attributeName3 = :attributeValue3", "(attributeName = :attributeValue OR (attributeName2 = :attributeValue2 AND attributeName3 = :attributeValue3))"},
		{"(attributeName = :attributeValue OR attributeName2 = :attributeValue2) AND attributeName3 = :attributeValue3", "((attributeName = :attributeValue OR attributeName2 = :attributeValue2) AND attributeName3 = :attributeValue3)"},
		{"a1 = :v1 AND a2 = :v2 OR a3 = :v3", "((a1 = :v1 AND a2 = :v2) OR a3 = :v3)"},
		{"a1 = :v1 AND NOT a2 = :v2 OR a3 = :v3", "((a1 = :v1 AND NOT a2 = :v2) OR a3 = :v3)"},
		{"size(Brand) <= :v_sub AND begins_with(Pictures.FrontView, :v_sub)", "(size(Brand) <= :v_sub AND begins_with(Pictures.FrontView, :v_sub))"},
	}

	for _, tt := range tests {
		l := lexer.New(strings.NewReader(tt.input))
		p := New(l)

		conditionExpr, err := p.ParseConditionExpression()
		if err != nil {
			t.Fatalf("unexpected error: %v when parsing %s", err, tt.input)
		}

		if conditionExpr.String() != tt.expected {
			t.Fatalf("expected %s but got %s", tt.expected, conditionExpr.String())
		}
	}
}

func TestParseUpdateExpression(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"SET attributeName = :attributeValue", "SET attributeName = :attributeValue"},
		{"SET ProductCategory = :c, Price = :p", "SET ProductCategory = :c, Price = :p"},
		{"SET RelatedItems[1] = :ri", "SET RelatedItems[1] = :ri"},
		{"SET #pr.#5star[1] = :r5, #pr.#3star = :r3", "SET #pr.#5star[1] = :r5, #pr.#3star = :r3"},
		{"SET Price = Price - :p", "SET Price = Price - :p"},
		{"SET Price = Price + :p", "SET Price = Price + :p"},
		{"SET #ri = list_append(#ri, :vals)", "SET #ri = list_append(#ri, :vals)"},
		{"SET #ri = list_append(:vals, #ri)", "SET #ri = list_append(:vals, #ri)"},
		{"SET Price = if_not_exists(Price, :p)", "SET Price = if_not_exists(Price, :p)"},
		{"REMOVE Brand, InStock, QuantityOnHand", "REMOVE Brand, InStock, QuantityOnHand"},
		{"REMOVE RelatedItems[1], RelatedItems[2]", "REMOVE RelatedItems[1], RelatedItems[2]"},
		{"ADD QuantityOnHand :q", "ADD QuantityOnHand :q"},
		{"DELETE Color :p", "DELETE Color :p"},
		{"SET Price = Price - :p REMOVE InStock", "SET Price = Price - :p REMOVE InStock"},
		{"SET RelatedItems[1] = :newValue, Price = :newPrice", "SET RelatedItems[1] = :newValue, Price = :newPrice"},
	}

	for _, tt := range tests {
		l := lexer.New(strings.NewReader(tt.input))
		p := New(l)

		exp, err := p.ParseUpdateExpression()
		if err != nil {
			t.Fatalf("unexpected error: %v when parsing %s", err, tt.input)
		}

		if exp.String() != tt.expected {
			t.Fatalf("expected %s but got %s", tt.expected, exp.String())
		}
	}

}
