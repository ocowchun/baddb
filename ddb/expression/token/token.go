package token

import (
	"fmt"
)

type TokenType uint8

const (
	ILLEGAL TokenType = iota
	EOF
	IDENT
	INT
	COMMA
	LPAREN
	RPAREN
	LBRACKET
	RBRACKET
	LT
	GT
	DOT
	BETWEEN
	AND
	OR
	NOT
	FALSE
	EQ
	NOT_EQ
	STRING
	COLON
	SHARP
	BEGINS_WITH
	ATTRIBUTE_EXISTS
	ATTRIBUTE_NOT_EXISTS
	ATTRIBUTE_TYPE
	CONTAINS
	SIZE
	IN
	IF_NOT_EXISTS
	PLUS
	MINUS
	SET
	LIST_APPEND
	REMOVE
	ADD
	DELETE
	EXPRESSION_ATTRIBUTE_NAME
	EXPRESSION_ATTRIBUTE_VALUE
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
	case LBRACKET:
		return "LBRACKET"
	case RBRACKET:
		return "RBRACKET"
	case LT:
		return "<"
	case GT:
		return ">"
	case DOT:
		return "."
	case BETWEEN:
		return "BETWEEN"
	case AND:
		return "AND"
	case OR:
		return "OR"
	case FALSE:
		return "FALSE"
	case EQ:
		return "EQ"
	case NOT_EQ:
		return "NOT_EQ"
	case STRING:
		return "STRING"
	case COLON:
		return ":"
	case SHARP:
		return "#"
	case BEGINS_WITH:
		return "begins_with"

	case ATTRIBUTE_EXISTS:
		return "attribute_exists"
	case ATTRIBUTE_NOT_EXISTS:
		return "attribute_not_exists"
	case ATTRIBUTE_TYPE:
		return "attribute_type"
	case CONTAINS:
		return "contains"
	case SIZE:
		return "size"
	case IN:
		return "IN"
	case IF_NOT_EXISTS:
		return "if_not_exists"
	case PLUS:
		return "+"
	case MINUS:
		return "-"
	case SET:
		return "SET"
	case LIST_APPEND:
		return "list_append"
	case REMOVE:
		return "REMOVE"
	case ADD:
		return "ADD"
	case DELETE:
		return "DELETE"
	case EXPRESSION_ATTRIBUTE_NAME:
		return "EXPRESSION_ATTRIBUTE_NAME"
	case EXPRESSION_ATTRIBUTE_VALUE:
		return "EXPRESSION_ATTRIBUTE_VALUE"

	default:
		panic(fmt.Sprintf("unknown token type: %d", t))
	}
}

type Token struct {
	Type    TokenType
	Literal string
}

var keywords = map[string]TokenType{
	"BETWEEN":              BETWEEN,
	"AND":                  AND,
	"OR":                   OR,
	"NOT":                  NOT,
	"IN":                   IN,
	"begins_with":          BEGINS_WITH,
	"attribute_exists":     ATTRIBUTE_EXISTS,
	"attribute_not_exists": ATTRIBUTE_NOT_EXISTS,
	"attribute_type":       ATTRIBUTE_TYPE,
	"contains":             CONTAINS,
	"size":                 SIZE,
	"if_not_exists":        IF_NOT_EXISTS,
	"SET":                  SET,
	"list_append":          LIST_APPEND,
	"REMOVE":               REMOVE,
	"ADD":                  ADD,
	"DELETE":               DELETE,
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}
