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
