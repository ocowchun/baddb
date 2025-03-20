package token

import "fmt"

type TokenType uint8

const (
	ILLEGAL TokenType = iota
	EOF
	IDENT
	INT
	COMMA
	LPAREN
	RPAREN
	LBRACE
	RBRACE
	LT
	GT
	DOT
	BETWEEN
	AND
	FALSE
	EQ
	NOT_EQ
	STRING
	COLON
	SHARP
	BEGINS_WITH
)

func (t TokenType) String() string {
	switch t {
	case ILLEGAL:
		return "ILLEGAL"
	case EOF:
		return "EOF"
	case IDENT:
		return "IDENT"
	case INT:
		return "INT"
	case COMMA:
		return "COMMA"
	case LPAREN:
		return "LPAREN"
	case RPAREN:
		return "RPAREN"
	case LBRACE:
		return "LBRACE"
	case RBRACE:
		return "RBRACE"
	case LT:
		return "LT"
	case GT:
		return "GT"
	case DOT:
		return "DOT"
	case BETWEEN:
		return "BETWEEN"
	case AND:
		return "AND"
	case FALSE:
		return "FALSE"
	case EQ:
		return "EQ"
	case NOT_EQ:
		return "NOT_EQ"
	case STRING:
		return "STRING"
	case COLON:
		return "COLON"
	case SHARP:
		return "SHARP"
	case BEGINS_WITH:
		return "begins_with"
	default:
		panic(fmt.Sprintf("unknown token type: %d", t))
	}
}

type Token struct {
	Type    TokenType
	Literal string
}

var keywords = map[string]TokenType{
	"BETWEEN":     BETWEEN,
	"AND":         AND,
	"begins_with": BEGINS_WITH,
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}
