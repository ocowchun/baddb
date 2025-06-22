package expression

import (
	"github.com/ocowchun/baddb/ddb/expression/ast"
	"github.com/ocowchun/baddb/ddb/expression/lexer"
	"github.com/ocowchun/baddb/ddb/expression/parser"
	"strings"
)

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html
func ParseKeyConditionExpression(content string) (*ast.KeyConditionExpression, error) {
	l := lexer.New(strings.NewReader(content))
	p := parser.New(l)

	return p.ParseKeyConditionExpression()
}
func ParseConditionExpression(content string) (ast.ConditionExpression, error) {
	l := lexer.New(strings.NewReader(content))
	p := parser.New(l)

	return p.ParseConditionExpression()
}

func ParseUpdateExpression(content string) (*ast.UpdateExpression, error) {
	l := lexer.New(strings.NewReader(content))
	p := parser.New(l)

	return p.ParseUpdateExpression()
}
