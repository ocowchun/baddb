package expression

import (
	"github.com/ocowchun/baddb/expression/ast"
	"github.com/ocowchun/baddb/expression/lexer"
	"github.com/ocowchun/baddb/expression/parser"
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
