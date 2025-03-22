package ast

import (
	"bytes"
	"fmt"
	"github.com/ocowchun/baddb/expression/token"
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
	Operator      string
	Value         *AttributeValueIdentifier
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

//function ::=
//    attribute_exists (path)
//    | attribute_not_exists (path)
//    | attribute_type (path, type)
//    | begins_with (path, substr)
//    | contains (path, operand)
//    | size (path)

type FunctionExpression interface {
	functionExpressionNode()
	String() string
}

type AttributeExistsFunctionExpression struct {
	Path Operand
}

func (fae *AttributeExistsFunctionExpression) functionExpressionNode() {}
func (fae *AttributeExistsFunctionExpression) String() string {
	var out bytes.Buffer
	out.WriteString("attribute_exists(")
	out.WriteString(fae.Path.String())
	out.WriteString(")")
	return out.String()
}

type AttributeNotExistsFunctionExpression struct {
	Path Operand
}

func (fane *AttributeNotExistsFunctionExpression) functionExpressionNode() {}
func (fane *AttributeNotExistsFunctionExpression) String() string {
	var out bytes.Buffer
	out.WriteString("attribute_not_exists(")
	out.WriteString(fane.Path.String())
	out.WriteString(")")
	return out.String()
}

type AttributeTypeFunctionExpression struct {
	Path Operand
	Type string
}

func (fate *AttributeTypeFunctionExpression) functionExpressionNode() {}
func (fate *AttributeTypeFunctionExpression) String() string {
	var out bytes.Buffer
	out.WriteString("attribute_type(")
	out.WriteString(fate.Path.String())
	out.WriteString(", ")
	out.WriteString(fate.Type)
	out.WriteString(")")
	return out.String()
}

type BeginsWithFunctionExpression struct {
	Path   Operand
	Prefix Operand
}

func (fbw *BeginsWithFunctionExpression) functionExpressionNode() {}
func (fbw *BeginsWithFunctionExpression) String() string {
	var out bytes.Buffer
	out.WriteString("begins_with(")
	out.WriteString(fbw.Path.String())
	out.WriteString(", ")
	out.WriteString(fbw.Prefix.String())
	out.WriteString(")")
	return out.String()
}

type ContainsFunctionExpression struct {
	Path    Operand
	Operand Operand
}

func (fc *ContainsFunctionExpression) functionExpressionNode() {}
func (fc *ContainsFunctionExpression) String() string {
	var out bytes.Buffer
	out.WriteString("contains(")
	out.WriteString(fc.Path.String())
	out.WriteString(", ")
	out.WriteString(fc.Operand.String())
	out.WriteString(")")
	return out.String()
}

type SizeFunctionExpression struct {
	Path Operand
}

func (fs *SizeFunctionExpression) functionExpressionNode() {}
func (fs *SizeFunctionExpression) String() string {
	var out bytes.Buffer
	out.WriteString("size(")
	out.WriteString(fs.Path.String())
	out.WriteString(")")
	return out.String()
}

//condition-expression ::=
//      operand comparator operand
//    | operand BETWEEN operand AND operand
//    | operand IN ( operand (',' operand (, ...) ))
//    | function
//    | condition AND condition
//    | condition OR condition
//    | NOT condition
//    | ( condition )

type Operand interface {
	operandNode()
	String() string
}

type AttributeNameOperand struct {
	Identifier *Identifier
	HasSharp   bool
	HasColon   bool
}

func (aop *AttributeNameOperand) operandNode() {}
func (aop *AttributeNameOperand) String() string {
	var out bytes.Buffer
	if aop.HasSharp {
		out.WriteString("#")
	}
	if aop.HasColon {
		out.WriteString(":")
	}
	out.WriteString(aop.Identifier.String())
	return out.String()
}

type IndexOperand struct {
	Left  Operand
	Index int
}

func (iop *IndexOperand) operandNode() {

}
func (iop *IndexOperand) String() string {
	return fmt.Sprintf("%s[%d]", iop.Left.String(), iop.Index)
}

type DotOperand struct {
	Left  Operand
	Right Operand
}

func (dop *DotOperand) operandNode() {}
func (dop *DotOperand) String() string {
	return fmt.Sprintf("%s.%s", dop.Left.String(), dop.Right.String())
}

type ConditionExpression interface {
	conditionExpressionNode()
	String() string
}

type ComparatorConditionExpression struct {
	Left     Operand
	Operator string
	Right    Operand
}

func (cc *ComparatorConditionExpression) conditionExpressionNode() {}
func (cc *ComparatorConditionExpression) String() string {
	var out bytes.Buffer
	out.WriteString(cc.Left.String())
	out.WriteString(" ")
	out.WriteString(cc.Operator)
	out.WriteString(" ")
	out.WriteString(cc.Right.String())
	return out.String()
}

type BetweenConditionExpression struct {
	Operand Operand
	Begin   Operand
	End     Operand
}

func (bc *BetweenConditionExpression) conditionExpressionNode() {}
func (bc *BetweenConditionExpression) String() string {
	var out bytes.Buffer
	out.WriteString(bc.Operand.String())
	out.WriteString(" BETWEEN ")
	out.WriteString(bc.Begin.String())
	out.WriteString(" AND ")
	out.WriteString(bc.End.String())
	return out.String()
}

type InConditionExpression struct {
	Operand Operand
	Values  []Operand
}

func (ic *InConditionExpression) conditionExpressionNode() {}
func (ic *InConditionExpression) String() string {
	var out bytes.Buffer
	out.WriteString(ic.Operand.String())
	out.WriteString(" IN (")
	for i, v := range ic.Values {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(v.String())
	}
	out.WriteString(")")
	return out.String()
}

type FunctionConditionExpression struct {
	Function FunctionExpression
}

func (fc *FunctionConditionExpression) conditionExpressionNode() {}
func (fc *FunctionConditionExpression) String() string {
	return fc.Function.String()
}

type AndConditionExpression struct {
	Left  ConditionExpression
	Right ConditionExpression
}

func (ac *AndConditionExpression) conditionExpressionNode() {}
func (ac *AndConditionExpression) String() string {
	var out bytes.Buffer
	out.WriteString("(")
	out.WriteString(ac.Left.String())
	out.WriteString(" AND ")
	out.WriteString(ac.Right.String())
	out.WriteString(")")
	return out.String()
}

type OrConditionExpression struct {
	Left  ConditionExpression
	Right ConditionExpression
}

func (oc *OrConditionExpression) conditionExpressionNode() {}
func (oc *OrConditionExpression) String() string {
	var out bytes.Buffer
	out.WriteString("(")
	out.WriteString(oc.Left.String())
	out.WriteString(" OR ")
	out.WriteString(oc.Right.String())
	out.WriteString(")")
	return out.String()
}

type NotConditionExpression struct {
	Condition ConditionExpression
}

func (nc *NotConditionExpression) conditionExpressionNode() {}
func (nc *NotConditionExpression) String() string {
	var out bytes.Buffer
	out.WriteString("NOT ")
	out.WriteString(nc.Condition.String())
	return out.String()
}
