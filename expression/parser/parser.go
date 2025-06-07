package parser

import (
	"errors"
	"fmt"
	"github.com/ocowchun/baddb/expression/ast"
	"github.com/ocowchun/baddb/expression/lexer"
	"github.com/ocowchun/baddb/expression/token"
	"strconv"
)

type Parser struct {
	l         *lexer.Lexer
	curToken  token.Token
	peekToken token.Token
}

func New(l *lexer.Lexer) *Parser {
	p := &Parser{
		l: l,
	}
	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()

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

func (p *Parser) parseIdentifier() (ast.Expression, error) {
	return &ast.Identifier{Token: p.curToken, Value: p.curToken.Literal}, nil
}

func (p *Parser) parseIntegerLiteral() (*ast.IntegerLiteral, error) {
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

// DynamoDB evaluates conditions from left to right using the following precedence rules:
// = <> < <= > >=
// IN
// BETWEEN
// attribute_exists attribute_not_exists begins_with contains
// Parentheses
// NOT
// AND
// OR
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Precedence
const (
	PRECEDENCE_LOWEST uint8 = iota
	PRECEDENCE_OR
	PRECEDENCE_AND
	PRECEDENCE_NOT
	PRECEDENCE_PARENTHESIS
	PRECEDENCE_BETWEEN
	PRECEDENCE_FUNCTION
	PRECEDENCE_IN
	PRECEDENCE_COMPARATOR
)

var precedences = map[token.TokenType]uint8{
	token.IN:                   PRECEDENCE_IN,
	token.BETWEEN:              PRECEDENCE_BETWEEN,
	token.ATTRIBUTE_EXISTS:     PRECEDENCE_FUNCTION,
	token.ATTRIBUTE_NOT_EXISTS: PRECEDENCE_FUNCTION,
	token.ATTRIBUTE_TYPE:       PRECEDENCE_FUNCTION,
	token.CONTAINS:             PRECEDENCE_FUNCTION,
	token.LPAREN:               PRECEDENCE_PARENTHESIS,
	token.NOT:                  PRECEDENCE_NOT,
	token.AND:                  PRECEDENCE_AND,
	token.OR:                   PRECEDENCE_OR,
}

func (p *Parser) peekPrecedence() uint8 {
	if p.peekToken.Literal == "<" || p.peekToken.Literal == ">" || p.peekToken.Literal == "=" {
		return PRECEDENCE_COMPARATOR
	}
	if p, ok := precedences[p.peekToken.Type]; ok {
		return p
	}

	return PRECEDENCE_LOWEST
}
func (p *Parser) curPrecedence() uint8 {
	if p.curToken.Literal == "<" || p.curToken.Literal == ">" || p.curToken.Literal == "=" {
		return PRECEDENCE_COMPARATOR
	}

	if p, ok := precedences[p.curToken.Type]; ok {
		return p
	}

	return PRECEDENCE_LOWEST
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

func (p *Parser) isFunctionCondition() bool {
	switch p.curToken.Type {
	case token.ATTRIBUTE_EXISTS:
		return true
	case token.ATTRIBUTE_NOT_EXISTS:
		return true
	case token.ATTRIBUTE_TYPE:
		return true
	case token.BEGINS_WITH:
		return true
	case token.CONTAINS:
		return true
	default:
		return false
	}
}

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
func (p *Parser) ParseConditionExpression() (ast.ConditionExpression, error) {
	return p.parseConditionExpression(PRECEDENCE_LOWEST)
}

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.FilterExpression
// The syntax for a filter expression is identical to that of a condition expression.
func (p *Parser) ParseScanExpression() (ast.ConditionExpression, error) {
	return p.parseConditionExpression(PRECEDENCE_LOWEST)
}

func (p *Parser) parseConditionExpression(precedence uint8) (ast.ConditionExpression, error) {
	var left ast.ConditionExpression
	var err error
	if p.curTokenIs(token.LPAREN) {
		p.nextToken()
		left, err = p.parseConditionExpression(precedence)
		if err != nil {
			return nil, err
		}
		if !p.expectPeek(token.RPAREN) {
			return nil, fmt.Errorf("failed to parse RPAREN")
		}
	} else if p.isFunctionCondition() {
		functionExpression, err := p.parseFunctionConditionExpression()
		if err != nil {
			return nil, err
		}

		left = &ast.FunctionConditionExpression{
			Function: functionExpression,
		}
	} else if p.curTokenIs(token.NOT) {
		p.nextToken()

		cond, err := p.parseConditionExpression(precedence)
		if err != nil {
			return nil, err
		}

		left = &ast.NotConditionExpression{
			Condition: cond,
		}
	} else {
		// it should be operand
		operand, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		if p.peekTokenIs(token.BETWEEN) {
			// | operand BETWEEN operand AND operand
			p.nextToken()
			p.nextToken()

			begin, err := p.parseOperand()
			if err != nil {
				return nil, err
			}

			if !p.expectPeek(token.AND) {
				return nil, fmt.Errorf("failed to parse AND")
			}
			p.nextToken()

			end, err := p.parseOperand()
			if err != nil {
				return nil, err
			}

			left = &ast.BetweenConditionExpression{
				Operand:    operand,
				LowerBound: begin,
				UpperBound: end,
			}
		} else if p.peekTokenIs(token.IN) {
			// | operand IN ( operand (',' operand (, ...) ))
			p.nextToken()

			if !p.expectPeek(token.LPAREN) {
				return nil, fmt.Errorf("failed to parse LPAREN")
			}
			p.nextToken()

			values := make([]ast.Operand, 0)
			for !p.curTokenIs(token.RPAREN) {
				value, err := p.parseOperand()
				if err != nil {
					return nil, err
				}
				values = append(values, value)

				if p.peekTokenIs(token.COMMA) {
					p.nextToken()
				}
				p.nextToken()
			}

			left = &ast.InConditionExpression{
				Operand: operand,
				Values:  values,
			}
		} else {
			//   operand comparator operand
			p.nextToken()
			op, err := p.parseOperator()
			if err != nil {
				return nil, err
			}
			p.nextToken()

			rightOperand, err := p.parseOperand()
			if err != nil {
				return nil, err
			}

			left = &ast.ComparatorConditionExpression{
				Left:     operand,
				Operator: op,
				Right:    rightOperand,
			}
		}
	}

	for (p.peekTokenIs(token.AND) || p.peekTokenIs(token.OR)) && precedence <= p.peekPrecedence() {
		p.nextToken()

		left, err = p.parseInfixConditionExpression(left)
		if err != nil {
			return nil, err
		}
	}

	return left, nil
}

func (p *Parser) parseInfixConditionExpression(left ast.ConditionExpression) (ast.ConditionExpression, error) {
	infixOp := p.curToken
	precedence := p.curPrecedence()
	p.nextToken()
	right, err := p.parseConditionExpression(precedence)
	if err != nil {
		return nil, err
	}

	switch infixOp.Type {
	case token.AND:
		return &ast.AndConditionExpression{
			Left:  left,
			Right: right,
		}, nil
	case token.OR:
		return &ast.OrConditionExpression{
			Left:  left,
			Right: right,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected token %v", infixOp)
	}
}

func (p *Parser) parseFunctionConditionExpression() (ast.FunctionExpression, error) {
	switch p.curToken.Type {
	case token.ATTRIBUTE_EXISTS:
		if !p.expectPeek(token.LPAREN) {
			return nil, fmt.Errorf("failed to parse LPAREN")
		}
		p.nextToken()

		path, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.RPAREN) {
			return nil, fmt.Errorf("failed to parse RPAREN")
		}

		return &ast.AttributeExistsFunctionExpression{
			Path: path,
		}, nil
	case token.ATTRIBUTE_NOT_EXISTS:
		if !p.expectPeek(token.LPAREN) {
			return nil, fmt.Errorf("failed to parse LPAREN")
		}
		p.nextToken()

		path, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.RPAREN) {
			return nil, fmt.Errorf("failed to parse RPAREN")
		}

		return &ast.AttributeNotExistsFunctionExpression{
			Path: path,
		}, nil
	case token.ATTRIBUTE_TYPE:
		if !p.expectPeek(token.LPAREN) {
			return nil, fmt.Errorf("failed to parse LPAREN")
		}
		p.nextToken()

		path, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.COMMA) {
			return nil, fmt.Errorf("failed to parse COMMA")
		}
		p.nextToken()

		// TODO: refactor parse type later
		i, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		identifier, ok := i.(*ast.Identifier)
		if !ok {
			return nil, fmt.Errorf("failed to parse identifier")
		}

		if !p.expectPeek(token.RPAREN) {
			return nil, fmt.Errorf("failed to parse RPAREN")
		}

		return &ast.AttributeTypeFunctionExpression{
			Path: path,
			Type: identifier.Value,
		}, nil
	case token.BEGINS_WITH:
		if !p.expectPeek(token.LPAREN) {
			return nil, fmt.Errorf("failed to parse LPAREN")
		}
		p.nextToken()

		path, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.COMMA) {
			return nil, fmt.Errorf("failed to parse COMMA")
		}
		p.nextToken()

		prefix, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.RPAREN) {
			return nil, fmt.Errorf("failed to parse RPAREN")
		}

		return &ast.BeginsWithFunctionExpression{
			Path:   path,
			Prefix: prefix,
		}, nil
	case token.CONTAINS:
		if !p.expectPeek(token.LPAREN) {
			return nil, fmt.Errorf("failed to parse LPAREN")
		}
		p.nextToken()

		path, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.COMMA) {
			return nil, fmt.Errorf("failed to parse COMMA")
		}
		p.nextToken()

		operand, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.RPAREN) {
			return nil, fmt.Errorf("failed to parse RPAREN")
		}

		return &ast.ContainsFunctionExpression{
			Path:    path,
			Operand: operand,
		}, nil
	default:
		return nil, fmt.Errorf("failed to parse function condition expression")
	}
}

func (p *Parser) parseAttributeNameOperand() (*ast.AttributeNameOperand, error) {
	if p.curTokenIs(token.IDENT) {
		i, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}

		identifier, ok := i.(*ast.Identifier)
		if !ok {
			return nil, fmt.Errorf("failed to parse identifier")
		}

		return &ast.AttributeNameOperand{
			Identifier: identifier,
		}, nil
	} else if p.curTokenIs(token.SHARP) {
		p.nextToken()
		i, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		identifier, ok := i.(*ast.Identifier)
		if !ok {
			return nil, fmt.Errorf("failed to parse identifier")
		}
		return &ast.AttributeNameOperand{
			Identifier: identifier,
			HasSharp:   true,
		}, nil
	} else if p.curTokenIs(token.COLON) {
		p.nextToken()
		i, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		identifier, ok := i.(*ast.Identifier)
		if !ok {
			return nil, fmt.Errorf("failed to parse identifier")
		}
		return &ast.AttributeNameOperand{
			Identifier: identifier,
			HasColon:   true,
		}, nil
	} else {
		return nil, fmt.Errorf("failed to parse attribute name")
	}
}

func (p *Parser) parsePathOperand() (ast.PathOperand, error) {
	var operand ast.PathOperand
	attributeNameOperand, err := p.parseAttributeNameOperand()
	if err != nil {
		return nil, err
	}
	operand = attributeNameOperand

	if p.peekTokenIs(token.DOT) {
		p.nextToken()
		p.nextToken()
		rightOperand, err := p.parsePathOperand()
		if err != nil {
			return nil, err
		}
		operand = &ast.DotOperand{
			Left:  operand,
			Right: rightOperand,
		}
	} else if p.peekTokenIs(token.LBRACKET) {
		p.nextToken()
		p.nextToken()

		i, err := p.parseIntegerLiteral()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.RBRACKET) {
			return nil, fmt.Errorf("failed to parse RBRACKET")
		}

		operand = &ast.IndexOperand{
			Left:  operand,
			Index: int(i.Value),
		}
	}

	return operand, nil
}

func (p *Parser) parseOperand() (ast.Operand, error) {
	if p.curTokenIs(token.SIZE) {
		if !p.expectPeek(token.LPAREN) {
			return nil, fmt.Errorf("failed to parse LPAREN")
		}
		p.nextToken()

		path, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.RPAREN) {
			return nil, fmt.Errorf("failed to parse RPAREN")
		}

		return &ast.SizeOperand{
			Path: path,
		}, nil

	} else {
		return p.parsePathOperand()
	}
}

func (p *Parser) ParseUpdateExpression() (*ast.UpdateExpression, error) {
	updateExpression := &ast.UpdateExpression{}
	for !p.curTokenIs(token.EOF) {
		switch p.curToken.Type {
		case token.SET:
			if updateExpression.Set != nil {
				return nil, fmt.Errorf("The \"SET\" section can only be used once in an update expression;")
			}
			set, err := p.parseSetClause()
			if err != nil {
				return nil, err
			}
			updateExpression.Set = set
		case token.REMOVE:
			if updateExpression.Remove != nil {
				return nil, fmt.Errorf("The \"REMOVE\" section can only be used once in an update expression;")
			}
			remove, err := p.parseRemoveClause()
			if err != nil {
				return nil, err
			}
			updateExpression.Remove = remove
		case token.ADD:
			if updateExpression.Add != nil {
				return nil, fmt.Errorf("The \"ADD\" section can only be used once in an update expression;")
			}

			add, err := p.parseAddClause()
			if err != nil {
				return nil, err
			}
			updateExpression.Add = add
		case token.DELETE:
			if updateExpression.Delete != nil {
				return nil, fmt.Errorf("The \"DELETE\" section can only be used once in an update expression;")
			}

			deleteClause, err := p.parseDeleteClause()
			if err != nil {
				return nil, err
			}
			updateExpression.Delete = deleteClause
		case token.EOF:
			break
		default:
			return nil, fmt.Errorf("unexpected token %v", p.curToken)
		}

		p.nextToken()

	}

	return updateExpression, nil
}

func (p *Parser) parseSetClause() (*ast.SetClause, error) {
	setClause := &ast.SetClause{
		Actions: make([]*ast.SetAction, 0),
	}
	for {
		p.nextToken()

		path, err := p.parseUpdateActionPath()
		if err != nil {
			return nil, err
		}

		if !p.expectPeek(token.EQ) {
			return nil, fmt.Errorf("expected EQ got %v", p.peekToken)
		}
		p.nextToken()

		value, err := p.parseSetActionValue()
		if err != nil {
			return nil, err
		}

		setClause.Actions = append(setClause.Actions, &ast.SetAction{
			Path:  path,
			Value: value,
		})

		if p.peekTokenIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	return setClause, nil
}

func (p *Parser) parseUpdateActionPath() (ast.UpdateActionPath, error) {
	op, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	path, ok := op.(ast.UpdateActionPath)
	if !ok {
		return nil, fmt.Errorf("failed to parse path")
	}

	return path, nil
}

func (p *Parser) parseSetActionValue() (ast.SetActionValue, error) {
	left, err := p.parseSetActionOperand()
	if err != nil {
		return nil, err
	}

	if p.peekTokenIs(token.PLUS) || p.peekTokenIs(token.MINUS) {
		p.nextToken()

		operator := p.curToken.Literal
		p.nextToken()

		right, err := p.parseSetActionOperand()
		if err != nil {
			return nil, err
		}

		return &ast.SetActionInfixExpression{
			Left:     left,
			Operator: operator,
			Right:    right,
		}, nil
	}

	return left, nil
}

func (p *Parser) parseSetActionOperand() (ast.SetActionOperand, error) {
	if p.curTokenIs(token.IF_NOT_EXISTS) {
		return p.parseIfNotExistsExpression()
	} else if p.curTokenIs(token.LIST_APPEND) {
		return p.parseListAppendExpression()
	}

	return p.parseUpdateActionPath()
}

func (p *Parser) parseIfNotExistsExpression() (*ast.IfNotExistsExpression, error) {
	if !p.curTokenIs(token.IF_NOT_EXISTS) {
		return nil, fmt.Errorf("expected if_not_exists got %v", p.curToken)
	}
	if !p.expectPeek(token.LPAREN) {
		return nil, fmt.Errorf("expected `(` got %v", p.peekToken)
	}
	p.nextToken()

	path, err := p.parseUpdateActionPath()
	if err != nil {
		return nil, err
	}

	if !p.expectPeek(token.COMMA) {
		return nil, fmt.Errorf("expected `,` got %v", p.peekToken)
	}
	p.nextToken()

	value, err := p.parseSetActionValue()
	if err != nil {
		return nil, err
	}

	if !p.expectPeek(token.RPAREN) {
		return nil, fmt.Errorf("expected `)` got %v", p.peekToken)
	}

	return &ast.IfNotExistsExpression{
		Path:  path,
		Value: value,
	}, nil
}

func (p *Parser) parseListAppendExpression() (*ast.ListAppendExpression, error) {
	if !p.curTokenIs(token.LIST_APPEND) {
		return nil, fmt.Errorf("expected list_append got %v", p.curToken)
	}
	if !p.expectPeek(token.LPAREN) {
		return nil, fmt.Errorf("expected `(` got %v", p.peekToken)
	}
	p.nextToken()

	target, err := p.parsePathOperand()
	if err != nil {
		return nil, err
	}

	if !p.expectPeek(token.COMMA) {
		return nil, fmt.Errorf("expected ',' got %v", p.peekToken)
	}
	p.nextToken()

	source, err := p.parsePathOperand()
	if err != nil {
		return nil, err
	}

	if !p.expectPeek(token.RPAREN) {
		return nil, fmt.Errorf("expected `)` got %v", p.peekToken)
	}

	return &ast.ListAppendExpression{
		Target: target,
		Source: source,
	}, nil
}

func (p *Parser) parseRemoveClause() (*ast.RemoveClause, error) {
	removeClause := &ast.RemoveClause{
		Paths: make([]ast.UpdateActionPath, 0),
	}
	for {
		p.nextToken()

		path, err := p.parseUpdateActionPath()

		if err != nil {
			return nil, err
		}
		removeClause.Paths = append(removeClause.Paths, path)

		if p.peekTokenIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	return removeClause, nil
}

func (p *Parser) parseAddClause() (*ast.AddClause, error) {
	addClause := &ast.AddClause{
		Actions: make([]*ast.AddAction, 0),
	}

	for {
		p.nextToken()

		attributeNameOperand, err := p.parseAttributeNameOperand()
		if err != nil {
			return nil, err
		} else if attributeNameOperand.HasColon {
			return nil, fmt.Errorf("Invalid UpdateExpression: Syntax error; token: \"%s\"", attributeNameOperand.String())
		}
		p.nextToken()

		value, err := p.parseUpdateActionPath()
		if err != nil {
			return nil, err
		}

		addClause.Actions = append(addClause.Actions, &ast.AddAction{
			Path:  attributeNameOperand,
			Value: value,
		})

		if p.peekTokenIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	return addClause, nil
}

func (p *Parser) parseDeleteClause() (*ast.DeleteClause, error) {
	deleteClause := &ast.DeleteClause{
		Actions: make([]*ast.DeleteAction, 0),
	}
	for {
		p.nextToken()

		attributeNameOperand, err := p.parseAttributeNameOperand()
		if err != nil {
			return nil, err
		} else if attributeNameOperand.HasColon {
			return nil, fmt.Errorf("Invalid UpdateExpression: Syntax error; token: \"%s\"", attributeNameOperand.String())
		}

		p.nextToken()
		subset, err := p.parseAttributeNameOperand()
		if err != nil {
			return nil, err
		} else if !subset.HasColon {
			return nil, fmt.Errorf("expected subset to have colon")
		}

		deleteClause.Actions = append(deleteClause.Actions, &ast.DeleteAction{
			Path:   attributeNameOperand,
			Subset: subset,
		})

		if p.peekTokenIs(token.COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	return deleteClause, nil
}
