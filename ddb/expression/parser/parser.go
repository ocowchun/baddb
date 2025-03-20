package parser

import (
	"errors"
	"fmt"
	"github.com/ocowchun/baddb/ddb/expression/ast"
	"github.com/ocowchun/baddb/ddb/expression/lexer"
	"github.com/ocowchun/baddb/ddb/expression/token"
	"strconv"
)

type Parser struct {
	l              *lexer.Lexer
	curToken       token.Token
	peekToken      token.Token
	prefixParseFns map[token.TokenType]prefixParseFn
	infixParseFns  map[token.TokenType]infixParseFn
}

func New(l *lexer.Lexer) *Parser {
	p := &Parser{
		l:              l,
		prefixParseFns: make(map[token.TokenType]prefixParseFn),
		infixParseFns:  make(map[token.TokenType]infixParseFn),
	}
	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()

	p.registerPrefix(token.IDENT, p.parseIdentifier)
	p.registerPrefix(token.INT, p.parseIntegerLiteral)
	p.registerPrefix(token.COLON, p.parseAttributeValueIdentifier)
	//p.registerPrefix(token.BANG, p.parsePrefixExpression)
	//p.registerPrefix(token.MINUS, p.parsePrefixExpression)
	//p.registerPrefix(token.TRUE, p.parseBoolean)
	//p.registerPrefix(token.FALSE, p.parseBoolean)
	p.registerPrefix(token.LPAREN, p.parseGroupedExpression)
	//p.registerPrefix(token.IF, p.parseIfExpression)
	//p.registerPrefix(token.FUNCTION, p.parseFunctionLiteral)
	p.registerPrefix(token.STRING, p.parseStringLiteral)
	//p.registerPrefix(token.LBRACKET, p.parseArrayLiteral)
	p.registerPrefix(token.LBRACE, p.parseHashLiteral)
	//p.registerPrefix(token.MACRO, p.parseMacroLiteral)

	//p.registerInfix(token.PLUS, p.parseInfixExpression)
	//p.registerInfix(token.MINUS, p.parseInfixExpression)
	//p.registerInfix(token.ASTERISK, p.parseInfixExpression)
	//p.registerInfix(token.SLASH, p.parseInfixExpression)
	p.registerInfix(token.EQ, p.parseInfixExpression)
	p.registerInfix(token.NOT_EQ, p.parseInfixExpression)
	p.registerInfix(token.LT, p.parseInfixExpression)
	p.registerInfix(token.GT, p.parseInfixExpression)
	p.registerInfix(token.LPAREN, p.parseCallExpression)
	p.registerInfix(token.AND, p.parseInfixExpression)
	//p.registerInfix(token.LBRACKET, p.parseIndexExpression)

	return p
}

func (p *Parser) ParseKeyConditionExpression() (*ast.KeyConditionExpression, error) {
	predicate1, err := p.parsePredicateExpression()
	if err != nil {
		return nil, err
	}
	keyCondExpression := &ast.KeyConditionExpression{
		Predicate1: predicate1,
	}
	if p.peekTokenIs(token.AND) {
		p.nextToken()
		p.nextToken()
		predicate2, err := p.parsePredicateExpression()
		if err != nil {
			return nil, err
		}
		keyCondExpression.Predicate2 = predicate2
	}

	return keyCondExpression, nil
}

func (p *Parser) parsePredicateExpression() (ast.PredicateExpression, error) {
	if p.curTokenIs(token.IDENT) || p.curTokenIs(token.SHARP) {
		attributeName, err := p.parseAttributeName()
		if err != nil {
			return nil, err
		}

		p.nextToken()
		if p.curTokenIs(token.BETWEEN) {
			// sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2
			p.nextToken()
			i, err := p.parseAttributeValueIdentifier()
			if err != nil {
				return nil, err
			}
			leftValue, ok := i.(*ast.AttributeValueIdentifier)
			if !ok {
				return nil, fmt.Errorf("failed to parse identifier")
			}
			if !p.expectPeek(token.AND) {
				return nil, fmt.Errorf("failed to parse BETWEEN")
			}
			p.nextToken()

			i, err = p.parseAttributeValueIdentifier()
			if err != nil {
				return nil, err
			}
			rightValue, ok := i.(*ast.AttributeValueIdentifier)
			if !ok {
				return nil, fmt.Errorf("failed to parse identifier")
			}
			betweenPredicateExpression := &ast.BetweenPredicateExpression{
				AttributeName: attributeName,
				LeftValue:     leftValue,
				RightValue:    rightValue,
			}
			return betweenPredicateExpression, nil
		} else {

			op, err := p.parseOperator()
			if err != nil {
				return nil, err
			}
			p.nextToken()

			i, err := p.parseAttributeValueIdentifier()
			if err != nil {
				return nil, err
			}
			val, ok := i.(*ast.AttributeValueIdentifier)
			if !ok {
				return nil, fmt.Errorf("failed to parse attribute value")
			}

			simplePredicate := &ast.SimplePredicateExpression{
				AttributeName: attributeName,
				Operator:      op,
				Value:         val,
			}
			return simplePredicate, nil
		}

	} else if p.curTokenIs(token.BEGINS_WITH) {
		// begins_with ( sortKeyName, :sortkeyval )
		if !p.expectPeek(token.LPAREN) {
			return nil, errors.New("failed to parse BEGINS_WITH")
		}
		p.nextToken()
		attributeName, err := p.parseAttributeName()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.COMMA) {
			return nil, errors.New("failed to parse BEGINS_WITH")
		}
		p.nextToken()

		i, err := p.parseAttributeValueIdentifier()
		if err != nil {
			return nil, err
		}

		val, ok := i.(*ast.AttributeValueIdentifier)
		if !ok {
			return nil, fmt.Errorf("failed to parse attribute value")
		}

		beginsWithPredicate := &ast.BeginsWithPredicateExpression{
			AttributeName: attributeName,
			Value:         val,
		}
		return beginsWithPredicate, nil
	} else {
		return nil, fmt.Errorf("unexpected token %v", p.curToken)
	}

	return nil, errors.New("not implemented")
}

func (p *Parser) parseAttributeName() (ast.AttributeName, error) {
	if p.curTokenIs(token.IDENT) {
		i, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		identifier, ok := i.(*ast.Identifier)
		if !ok {
			return nil, fmt.Errorf("failed to parse identifier")
		}
		return identifier, nil
	} else if p.curTokenIs(token.SHARP) {
		attributeNameIdentifier := &ast.AttributeNameIdentifier{
			Token: p.curToken,
		}

		p.nextToken()
		i, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		identifier, ok := i.(*ast.Identifier)
		if !ok {
			return nil, fmt.Errorf("failed to parse identifier")
		}
		attributeNameIdentifier.Name = identifier

		return attributeNameIdentifier, nil
	}

	return nil, fmt.Errorf("unexpected token %v", p.curToken)
}

func (p *Parser) parseOperator() (string, error) {
	op := ""

	if p.curTokenIs(token.LT) {
		op += "<"

	} else if p.curTokenIs(token.GT) {
		op += ">"
	}

	if p.curTokenIs(token.EQ) {
		op += "="

	} else if op != "" && p.peekTokenIs(token.EQ) {
		p.nextToken()
		op += "="
	} else if op == "" {
		return "", fmt.Errorf("unexpected token %v", p.curToken)
	}

	return op, nil
}

func (p *Parser) registerPrefix(tokenType token.TokenType, fn prefixParseFn) {
	p.prefixParseFns[tokenType] = fn
}

func (p *Parser) registerInfix(tokenType token.TokenType, fn infixParseFn) {
	p.infixParseFns[tokenType] = fn
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) curTokenIs(t token.TokenType) bool {
	return p.curToken.Type == t
}
func (p *Parser) peekTokenIs(t token.TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(tokenType token.TokenType) bool {
	if p.peekTokenIs(tokenType) {
		p.nextToken()
		return true
	}
	return false
}

type (
	prefixParseFn func() (ast.Expression, error)
	infixParseFn  func(ast.Expression) (ast.Expression, error)
)

const (
	_ uint8 = iota
	LOWEST
	EQUALS
	LESSGREATER
	SUM
	PRODUCT
	PREFIX
	CALL
	INDEX
)

func (p *Parser) parseExpressionStatement() (*ast.ExpressionStatement, error) {
	stmt := &ast.ExpressionStatement{Token: p.curToken}
	expression, err := p.parseExpression(LOWEST)
	stmt.Expression = expression
	if err != nil {
		return nil, err
	}

	return stmt, nil
}

func (p *Parser) parseExpression(precedence uint8) (ast.Expression, error) {
	prefix := p.prefixParseFns[p.curToken.Type]
	if prefix == nil {
		return nil, fmt.Errorf("no prefix parse fuction for %s found", p.curToken.Type)
	}
	leftExp, err := prefix()
	if err != nil {
		return nil, err
	}

	//for !p.peekTokenIs(token.SEMICOLON) && precedence < p.peekPrecedence() {
	for precedence < p.peekPrecedence() {
		infix := p.infixParseFns[p.peekToken.Type]
		if infix == nil {
			return leftExp, nil
		}

		p.nextToken()
		leftExp, err = infix(leftExp)
		if err != nil {
			return nil, err
		}
	}

	return leftExp, nil
}
func (p *Parser) parseIdentifier() (ast.Expression, error) {
	return &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}, nil
}

func (p *Parser) parseIntegerLiteral() (ast.Expression, error) {
	lit := &ast.IntegerLiteral{Token: p.curToken}
	value, err := strconv.ParseInt(p.curToken.Literal, 0, 64)
	if err != nil {
		return nil, err
	}
	lit.Value = value
	return lit, nil
}

func (p *Parser) parseAttributeValueIdentifier() (ast.Expression, error) {
	avi := &ast.AttributeValueIdentifier{Token: p.curToken}
	p.nextToken()
	i, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	identifier, ok := i.(*ast.Identifier)
	if !ok {
		return nil, fmt.Errorf("failed to parse identifier")
	}

	avi.Name = identifier

	return avi, nil
}

func (p *Parser) parsePrefixExpression() (ast.Expression, error) {
	expression := &ast.PrefixExpression{Token: p.curToken, Operator: p.curToken.Literal}
	p.nextToken()
	right, err := p.parseExpression(PREFIX)
	if err != nil {
		return nil, err
	}
	expression.Right = right

	return expression, nil
}

var precedences = map[token.TokenType]uint8{
	token.EQ:     EQUALS,
	token.NOT_EQ: EQUALS,
	token.LT:     LESSGREATER,
	token.GT:     LESSGREATER,
	//token.PLUS:     SUM,
	//token.MINUS:    SUM,
	//token.ASTERISK: PRODUCT,
	//token.SLASH:    PRODUCT,
	token.LPAREN: CALL,
	//token.LBRACKET: INDEX,
}

func (p *Parser) peekPrecedence() uint8 {
	if p, ok := precedences[p.peekToken.Type]; ok {
		return p
	}
	return LOWEST
}
func (p *Parser) curPrecedence() uint8 {
	if p, ok := precedences[p.curToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) parseInfixExpression(left ast.Expression) (ast.Expression, error) {
	expression := &ast.InfixExpression{
		Token:    p.curToken,
		Operator: p.curToken.Literal,
		Left:     left,
	}
	precedence := p.curPrecedence()
	p.nextToken()
	right, err := p.parseExpression(precedence)
	if err != nil {
		return nil, err
	}
	expression.Right = right

	return expression, nil
}

//func (p *Parser) parseBoolean() (ast.Expression, error) {
//	expression := &ast.Boolean{Token: p.curToken}
//	if p.curTokenIs(token.TRUE) {
//		expression.Value = true
//	} else if p.curTokenIs(token.FALSE) {
//		expression.Value = false
//	} else {
//		return nil, fmt.Errorf("expected true or false, got %s", p.curToken.Literal)
//	}
//
//	return expression, nil
//}

func (p *Parser) parseGroupedExpression() (ast.Expression, error) {
	p.nextToken()
	exp, err := p.parseExpression(LOWEST)
	if err != nil {
		return nil, err
	}
	if !p.expectPeek(token.RPAREN) {
		return nil, fmt.Errorf("expected next token to be right parenthesis got %s", p.curToken.Literal)
	}

	return exp, nil
}

func (p *Parser) parseParameters() ([]*ast.Identifier, error) {
	var parameters []*ast.Identifier
	p.nextToken()
	for !p.curTokenIs(token.RPAREN) {
		i, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		identifier, ok := i.(*ast.Identifier)
		if !ok {
			return nil, fmt.Errorf("failed to parse identifier")
		}

		parameters = append(parameters, identifier)
		p.nextToken()
		if p.curTokenIs(token.COMMA) {
			p.nextToken()
		}
	}

	return parameters, nil
}

func (p *Parser) parseCallExpression(function ast.Expression) (ast.Expression, error) {
	exp := &ast.CallExpression{Token: p.curToken, Function: function}
	arguments, err := p.parseCallArguments()
	if err != nil {
		return nil, err
	}
	exp.Arguments = arguments

	return exp, nil
}

func (p *Parser) parseCallArguments() ([]ast.Expression, error) {
	return p.parseExpressionLists(token.RPAREN)
}

func (p *Parser) parseExpressionLists(endTokenType token.TokenType) ([]ast.Expression, error) {
	var args []ast.Expression
	p.nextToken()
	for !p.curTokenIs(endTokenType) {
		arg, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		if p.peekTokenIs(token.COMMA) {
			p.nextToken()
		}
		p.nextToken()

	}

	return args, nil
}

func (p *Parser) parseStringLiteral() (ast.Expression, error) {
	return &ast.StringLiteral{Token: p.curToken, Value: p.curToken.Literal}, nil
}

//func (p *Parser) parseArrayLiteral() (ast.Expression, error) {
//	array := &ast.ArrayLiteral{Token: p.curToken}
//	elements, err := p.parseArrayElements()
//	if err != nil {
//		return nil, err
//	}
//	array.Elements = elements
//
//	return array, nil
//}

//func (p *Parser) parseArrayElements() ([]ast.Expression, error) {
//	return p.parseExpressionLists(token.RBRACKET)
//}

//func (p *Parser) parseIndexExpression(left ast.Expression) (ast.Expression, error) {
//	indexExpression := &ast.IndexExpression{Token: p.curToken, Left: left}
//	p.nextToken()
//	exp, err := p.parseExpression(LOWEST)
//	if err != nil {
//		return nil, err
//	}
//	indexExpression.Index = exp
//	if !p.expectPeek(token.RBRACKET) {
//		return nil, fmt.Errorf("expected next token to be right bracket got %s", p.peekToken.Literal)
//	}
//
//	return indexExpression, nil
//}

func (p *Parser) parseHashLiteral() (ast.Expression, error) {
	hash := &ast.HashLiteral{Token: p.curToken}
	p.nextToken()
	pairs := make(map[ast.Expression]ast.Expression)
	for !p.curTokenIs(token.RBRACE) {
		key, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.COLON) {
			return nil, fmt.Errorf("expected next token to be colon got %s", p.peekToken.Literal)
		}
		p.nextToken()

		val, err := p.parseExpression(LOWEST)
		if err != nil {
			return nil, err
		}

		pairs[key] = val
		if p.peekTokenIs(token.COMMA) {
			p.nextToken()
			p.nextToken()
		} else if p.peekTokenIs(token.RBRACE) {
			p.nextToken()
			break

		}

	}
	hash.Pairs = pairs

	return hash, nil
}
