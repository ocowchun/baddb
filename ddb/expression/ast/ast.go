package ast

import (
	"bytes"
	"fmt"
	"github.com/ocowchun/baddb/ddb/expression/token"
	"strings"
)

type Node interface {
	TokenLiteral() string
	String() string
}

type Statement interface {
	Node
	statementNode()
}

type Expression interface {
	Node
	expressionNode()
}

type Program struct {
	Statements []Statement
}

func (p *Program) TokenLiteral() string {
	if len(p.Statements) > 0 {
		return p.Statements[0].TokenLiteral()
	} else {
		return ""
	}
}

func (p *Program) String() string {
	var out bytes.Buffer
	for _, s := range p.Statements {
		out.WriteString(s.String())
	}
	return out.String()
}

type Identifier struct {
	Token token.Token
	Value string
}

func (i *Identifier) attributeNameNode() {}
func (i *Identifier) expressionNode()    {}
func (i *Identifier) TokenLiteral() string {
	return i.Token.Literal
}
func (i *Identifier) String() string {
	return i.Value
}

type AttributeNameIdentifier struct {
	Token token.Token
	Name  *Identifier
}

func (avi *AttributeNameIdentifier) attributeNameNode() {}
func (avi *AttributeNameIdentifier) expressionNode()    {}
func (avi *AttributeNameIdentifier) TokenLiteral() string {
	return avi.Token.Literal
}
func (avi *AttributeNameIdentifier) String() string {
	return fmt.Sprintf("#%s", avi.Name)
}

type AttributeValueIdentifier struct {
	Token token.Token
	Name  *Identifier
}

func (avi *AttributeValueIdentifier) expressionNode() {}
func (avi *AttributeValueIdentifier) TokenLiteral() string {
	return avi.Token.Literal
}
func (avi *AttributeValueIdentifier) String() string {
	return fmt.Sprintf(":%s", avi.Name)
}

type LetStatement struct {
	Token token.Token
	Name  *Identifier
	Value Expression
}

func (ls *LetStatement) statementNode() {}
func (ls *LetStatement) TokenLiteral() string {
	return ls.Token.Literal
}
func (ls *LetStatement) String() string {
	var out bytes.Buffer
	out.WriteString(ls.TokenLiteral() + " ")
	out.WriteString(ls.Name.String())
	out.WriteString(" = ")
	if ls.Value != nil {
		out.WriteString(ls.Value.String())
	}
	out.WriteString(";")
	return out.String()
}

type ReturnStatement struct {
	Token       token.Token
	ReturnValue Expression
}

func (rs *ReturnStatement) statementNode() {
}
func (rs *ReturnStatement) TokenLiteral() string {
	return rs.Token.Literal
}
func (rs *ReturnStatement) String() string {
	var out bytes.Buffer
	out.WriteString(rs.TokenLiteral() + " ")
	if rs.ReturnValue != nil {
		out.WriteString(rs.ReturnValue.String())
	}
	out.WriteString(";")
	return out.String()
}

type ExpressionStatement struct {
	Token      token.Token
	Expression Expression
}

func (es *ExpressionStatement) statementNode() {}
func (es *ExpressionStatement) TokenLiteral() string {
	return es.Token.Literal
}
func (es *ExpressionStatement) String() string {
	if es.Expression != nil {
		return es.Expression.String()
	}
	return ""
}

// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression

type PredicateType uint8

const (
	SIMPLE PredicateType = iota
	BETWEEN
	BEGINS_WITH
)

type PredicateExpression interface {
	PredicateType() PredicateType
	expressionNode()
	TokenLiteral() string
	String() string
}

type AttributeName interface {
	attributeNameNode()
	String() string
}

type SimplePredicateExpression struct {
	Token         token.Token
	AttributeName AttributeName
	//Name                    *Identifier
	//AttributeNameIdentifier *AttributeNameIdentifier
	Operator string
	Value    *AttributeValueIdentifier
}

func (pe *SimplePredicateExpression) PredicateType() PredicateType {
	return SIMPLE
}
func (pe *SimplePredicateExpression) expressionNode() {}
func (pe *SimplePredicateExpression) TokenLiteral() string {
	return pe.Token.Literal
}

func (pe *SimplePredicateExpression) String() string {
	var out bytes.Buffer
	out.WriteString(pe.AttributeName.String())
	out.WriteString(" ")
	out.WriteString(pe.Operator)
	out.WriteString(" ")
	out.WriteString(pe.Value.String())
	return out.String()
}

type BetweenPredicateExpression struct {
	Token         token.Token
	AttributeName AttributeName
	LeftValue     *AttributeValueIdentifier
	RightValue    *AttributeValueIdentifier
}

func (pe *BetweenPredicateExpression) PredicateType() PredicateType {
	return BETWEEN
}
func (pe *BetweenPredicateExpression) expressionNode() {}
func (pe *BetweenPredicateExpression) TokenLiteral() string {
	return pe.Token.Literal
}

func (pe *BetweenPredicateExpression) String() string {
	var out bytes.Buffer
	out.WriteString(pe.AttributeName.String())
	out.WriteString(" BETWEEN ")
	out.WriteString(pe.LeftValue.String())
	out.WriteString(" AND ")
	out.WriteString(pe.RightValue.String())
	return out.String()
}

type BeginsWithPredicateExpression struct {
	Token token.Token
	//Name                    *Identifier
	//AttributeNameIdentifier *AttributeNameIdentifier
	AttributeName AttributeName
	Value         *AttributeValueIdentifier
}

func (pe *BeginsWithPredicateExpression) PredicateType() PredicateType {
	return BEGINS_WITH
}
func (pe *BeginsWithPredicateExpression) expressionNode() {}
func (pe *BeginsWithPredicateExpression) TokenLiteral() string {
	return pe.Token.Literal
}

func (pe *BeginsWithPredicateExpression) String() string {
	var out bytes.Buffer
	out.WriteString("begins_with(")
	out.WriteString(pe.AttributeName.String())
	out.WriteString(", ")
	out.WriteString(pe.Value.String())
	out.WriteString(")")
	return out.String()
}

type KeyConditionExpression struct {
	Token      token.Token
	Predicate1 PredicateExpression
	Predicate2 PredicateExpression
}

func (kce *KeyConditionExpression) expressionNode() {}
func (kce *KeyConditionExpression) TokenLiteral() string {
	return kce.Token.Literal
}
func (kce *KeyConditionExpression) String() string {
	var out bytes.Buffer
	//out.WriteString(kce.TokenLiteral())
	out.WriteString(kce.Predicate1.String())
	if kce.Predicate2 != nil {
		out.WriteString(" ")
		out.WriteString("AND")
		out.WriteString(" ")
		out.WriteString(kce.Predicate2.String())
	}

	return out.String()
}

type IntegerLiteral struct {
	Token token.Token
	Value int64
}

func (il *IntegerLiteral) expressionNode() {}
func (il *IntegerLiteral) TokenLiteral() string {
	return il.Token.Literal
}
func (il *IntegerLiteral) String() string {
	return il.Token.Literal
}

type PrefixExpression struct {
	Token    token.Token // The prefix token, e.g. !
	Operator string
	Right    Expression
}

func (pe *PrefixExpression) expressionNode() {}
func (pe *PrefixExpression) TokenLiteral() string {
	return pe.Token.Literal
}
func (pe *PrefixExpression) String() string {
	var out bytes.Buffer
	out.WriteString("(")
	out.WriteString(pe.Operator)
	out.WriteString(pe.Right.String())
	out.WriteString(")")
	return out.String()
}

type InfixExpression struct {
	Token    token.Token
	Left     Expression
	Operator string
	Right    Expression
}

func (ie *InfixExpression) expressionNode() {}
func (ie *InfixExpression) TokenLiteral() string {
	return ie.Token.Literal
}
func (ie *InfixExpression) String() string {
	var out bytes.Buffer
	out.WriteString("(")
	out.WriteString(ie.Left.String())
	out.WriteString(" " + ie.Operator + " ")
	out.WriteString(ie.Right.String())
	out.WriteString(")")
	return out.String()
}

type Boolean struct {
	Token token.Token
	Value bool
}

func (b *Boolean) expressionNode() {}
func (b *Boolean) TokenLiteral() string {
	return b.Token.Literal
}
func (b *Boolean) String() string {
	return b.Token.Literal
}

type BlockStatement struct {
	Token      token.Token
	Statements []Statement
}

func (bs *BlockStatement) statementNode() {}
func (bs *BlockStatement) TokenLiteral() string {
	return bs.Token.Literal
}
func (bs *BlockStatement) String() string {
	var out bytes.Buffer
	for _, s := range bs.Statements {
		out.WriteString(s.String())
	}
	return out.String()
}

type IfExpression struct {
	Token       token.Token
	Condition   Expression
	Consequence *BlockStatement
	Alternative *BlockStatement
}

func (ie *IfExpression) expressionNode() {}
func (ie *IfExpression) TokenLiteral() string {
	return ie.Token.Literal
}
func (ie *IfExpression) String() string {
	var out bytes.Buffer
	out.WriteString("if")
	out.WriteString(ie.Condition.String())
	out.WriteString(" ")
	out.WriteString(ie.Consequence.String())
	if ie.Alternative != nil {
		out.WriteString("else ")
		out.WriteString(ie.Alternative.String())
	}
	return out.String()
}

type FunctionLiteral struct {
	Token      token.Token
	Parameters []*Identifier
	Body       *BlockStatement
	Name       string
}

func (fl *FunctionLiteral) expressionNode() {}
func (fl *FunctionLiteral) TokenLiteral() string {
	return fl.Token.Literal
}
func (fl *FunctionLiteral) String() string {
	var out bytes.Buffer
	var params []string
	for _, p := range fl.Parameters {
		params = append(params, p.String())
	}
	out.WriteString(fl.TokenLiteral())
	if fl.Name != "" {
		out.WriteString(fmt.Sprintf(" <%s>", fl.Name))
	}
	out.WriteString("(")
	out.WriteString(strings.Join(params, ", "))
	out.WriteString(")")
	out.WriteString(fl.Body.String())
	return out.String()
}

type CallExpression struct {
	Token     token.Token
	Function  Expression
	Arguments []Expression
}

func (ce *CallExpression) expressionNode() {}
func (ce *CallExpression) TokenLiteral() string {
	return ce.Token.Literal
}
func (ce *CallExpression) String() string {
	var out bytes.Buffer
	var args []string
	for _, a := range ce.Arguments {
		args = append(args, a.String())
	}
	out.WriteString(ce.Function.String())
	out.WriteString("(")
	out.WriteString(strings.Join(args, ", "))
	out.WriteString(")")
	return out.String()
}

type StringLiteral struct {
	Token token.Token
	Value string
}

func (sl *StringLiteral) expressionNode() {}
func (sl *StringLiteral) TokenLiteral() string {
	return sl.Token.Literal
}
func (sl *StringLiteral) String() string {
	return sl.Token.Literal
}

type ArrayLiteral struct {
	Token    token.Token
	Elements []Expression
}

func (al *ArrayLiteral) expressionNode() {}
func (al *ArrayLiteral) TokenLiteral() string {
	return al.Token.Literal
}
func (al *ArrayLiteral) String() string {
	var out bytes.Buffer
	elements := make([]string, len(al.Elements))
	for i, el := range al.Elements {
		elements[i] = el.String()
	}
	out.WriteString("[")
	out.WriteString(strings.Join(elements, ", "))
	out.WriteString("]")
	return out.String()
}

type IndexExpression struct {
	Token token.Token
	Left  Expression
	Index Expression
}

func (ie *IndexExpression) expressionNode() {}
func (ie *IndexExpression) TokenLiteral() string {
	return ie.Token.Literal
}
func (ie *IndexExpression) String() string {
	var out bytes.Buffer
	out.WriteString("(")
	out.WriteString(ie.Left.String())
	out.WriteString("[")
	out.WriteString(ie.Index.String())
	out.WriteString("])")
	return out.String()
}

type HashLiteral struct {
	Token token.Token
	Pairs map[Expression]Expression
}

func (hl *HashLiteral) expressionNode() {}
func (hl *HashLiteral) TokenLiteral() string {
	return hl.Token.Literal
}
func (hl *HashLiteral) String() string {
	var output bytes.Buffer
	var pairs []string
	for key, value := range hl.Pairs {
		pairs = append(pairs, key.String()+":"+value.String())
	}
	output.WriteString("{")
	output.WriteString(strings.Join(pairs, ", "))
	output.WriteString("}")
	return output.String()
}

type MacroLiteral struct {
	Token      token.Token
	Parameters []*Identifier
	Body       *BlockStatement
}

func (ml *MacroLiteral) expressionNode() {}
func (ml *MacroLiteral) TokenLiteral() string {
	return ml.Token.Literal
}
func (ml *MacroLiteral) String() string {
	var output bytes.Buffer
	var params = make([]string, len(ml.Parameters))
	for i, p := range ml.Parameters {
		params[i] = p.String()
	}
	output.WriteString(ml.TokenLiteral())
	output.WriteString("(")
	output.WriteString(strings.Join(params, ", "))
	output.WriteString(")")
	output.WriteString(ml.Body.String())
	return output.String()
}
