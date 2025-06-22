package lexer

import (
	"github.com/ocowchun/baddb/ddb/expression/token"
	"strings"
	"testing"
)

func TestLexer_NextToken(t *testing.T) {
	content := `
Id = :v1 AND PostedBy BETWEEN :v2a AND :v2b SET #pr.#5star[1] = :r5
`
	input := strings.NewReader(content)
	tests := []struct {
		expectedType    token.TokenType
		expectedLiteral string
	}{
		{token.IDENT, "Id"},
		{token.EQ, "="},
		{token.COLON, ":"},
		{token.IDENT, "v1"},
		{token.AND, "AND"},
		{token.IDENT, "PostedBy"},
		{token.BETWEEN, "BETWEEN"},
		{token.COLON, ":"},
		{token.IDENT, "v2a"},
		{token.AND, "AND"},
		{token.COLON, ":"},
		{token.IDENT, "v2b"},
		{token.SET, "SET"},
		{token.SHARP, "#"},
		{token.IDENT, "pr"},
		{token.DOT, "."},
		{token.SHARP, "#"},
		{token.IDENT, "5star"},
		{token.LBRACKET, "["},
		{token.INT, "1"},
		{token.RBRACKET, "]"},
		{token.EQ, "="},
		{token.COLON, ":"},
		{token.IDENT, "r5"},
		{token.EOF, ""},
	}

	lexer := New(input)

	for i, tt := range tests {
		tok := lexer.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - expected token type '%s', got '%s'", i, tt.expectedType, tok.Type)
		}
		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - expected literal '%s', got '%s'", i, tt.expectedLiteral, tok.Literal)
		}
	}
}
