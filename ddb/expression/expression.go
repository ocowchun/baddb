package expression

import (
	"github.com/ocowchun/baddb/ddb/expression/ast"
	"github.com/ocowchun/baddb/ddb/expression/lexer"
	"github.com/ocowchun/baddb/ddb/expression/parser"
	"strings"
)

//https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html

func lex(expression string) {
	// Id = :v1 AND PostedBy BETWEEN :v2a AND :v2b

}

func ParseKeyConditionExpression(content string) (*ast.KeyConditionExpression, error) {
	l := lexer.New(strings.NewReader(content))
	p := parser.New(l)

	return p.ParseKeyConditionExpression()

	//kce := &ast.KeyConditionExpression{
	//	Token: token.Token{
	//		Type:    token.IDENT,
	//		Literal: content,
	//	},
	//	Predicate1: nil,
	//	Predicate2: nil,
	//}
	//
	//fmt.Println(kce)
	//// TODO: implement it
	//return nil, errors.New("unimplemented")
}
