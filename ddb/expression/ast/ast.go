package ast

import (
	"bytes"
	"fmt"
	"github.com/ocowchun/baddb/ddb/expression/token"
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
	Token         token.Token
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

func (il *IntegerLiteral) TokenLiteral() string {
	return il.Token.Literal
}
func (il *IntegerLiteral) String() string {
	return il.Token.Literal
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

type Operand interface {
	operandNode()
	String() string
}

type PathOperand interface {
	Operand
	SetActionValue
	pathOperand()
}

type AttributeNameOperand struct {
	Identifier *Identifier
	HasSharp   bool
	HasColon   bool
}

func (aop *AttributeNameOperand) operandNode()          {}
func (aop *AttributeNameOperand) updateActionPath()     {}
func (aop *AttributeNameOperand) pathOperand()          {}
func (aop *AttributeNameOperand) setActionOperandNode() {}
func (aop *AttributeNameOperand) setActionValue()       {}
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
	Left  PathOperand
	Index int
}

func (iop *IndexOperand) operandNode()          {}
func (iop *IndexOperand) updateActionPath()     {}
func (iop *IndexOperand) pathOperand()          {}
func (iop *IndexOperand) setActionOperandNode() {}
func (iop *IndexOperand) setActionValue()       {}
func (iop *IndexOperand) String() string {
	return fmt.Sprintf("%s[%d]", iop.Left.String(), iop.Index)
}

type DotOperand struct {
	//Left  Operand
	//Right Operand
	Left  PathOperand
	Right PathOperand
}

func (dop *DotOperand) operandNode()          {}
func (dop *DotOperand) updateActionPath()     {}
func (dop *DotOperand) pathOperand()          {}
func (dop *DotOperand) setActionOperandNode() {}
func (dop *DotOperand) setActionValue()       {}
func (dop *DotOperand) String() string {
	return fmt.Sprintf("%s.%s", dop.Left.String(), dop.Right.String())
}

type SizeOperand struct {
	Path Operand
}

func (fop *SizeOperand) operandNode() {}
func (fop *SizeOperand) String() string {
	var out bytes.Buffer
	out.WriteString("size(")
	out.WriteString(fop.Path.String())
	out.WriteString(")")
	return out.String()
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
	Operand    Operand
	LowerBound Operand
	UpperBound Operand
}

func (bc *BetweenConditionExpression) conditionExpressionNode() {}
func (bc *BetweenConditionExpression) String() string {
	var out bytes.Buffer
	out.WriteString(bc.Operand.String())
	out.WriteString(" BETWEEN ")
	out.WriteString(bc.LowerBound.String())
	out.WriteString(" AND ")
	out.WriteString(bc.UpperBound.String())
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

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
type UpdateExpression struct {
	Set    *SetClause
	Remove *RemoveClause
	Add    *AddClause
	Delete *DeleteClause
}

func (exp *UpdateExpression) String() string {
	var out bytes.Buffer
	if exp.Set != nil {
		if out.Len() > 0 {
			out.WriteString(" ")
		}
		out.WriteString(exp.Set.String())
	}
	if exp.Remove != nil {
		if out.Len() > 0 {
			out.WriteString(" ")
		}
		out.WriteString(exp.Remove.String())
	}
	if exp.Add != nil {
		if out.Len() > 0 {
			out.WriteString(" ")
		}
		out.WriteString(exp.Add.String())
	}
	if exp.Delete != nil {
		if out.Len() > 0 {
			out.WriteString(" ")
		}
		out.WriteString(exp.Delete.String())
	}
	return out.String()
}

type SetClause struct {
	Actions []*SetAction
}

func (sc *SetClause) String() string {
	var out bytes.Buffer
	out.WriteString("SET ")
	for i, action := range sc.Actions {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(action.Path.String())
		out.WriteString(" = ")
		out.WriteString(action.Value.String())
	}
	return out.String()
}

type UpdateActionPath interface {
	//updateActionPath()
	PathOperand
	SetActionOperand
}

type SetActionOperand interface {
	setActionOperandNode()
	SetActionValue
}

type SetActionValue interface {
	setActionValue()
	String() string
}

type SetActionInfixExpression struct {
	Left     SetActionOperand
	Operator string
	Right    SetActionOperand
}

func (infix *SetActionInfixExpression) setActionValue() {}
func (infix *SetActionInfixExpression) String() string {
	var out bytes.Buffer
	out.WriteString(infix.Left.String())
	out.WriteString(" ")
	out.WriteString(infix.Operator)
	out.WriteString(" ")
	out.WriteString(infix.Right.String())
	return out.String()
}

type SetActionFunction interface {
	setActionFunctionNode()
	SetActionOperand
}

type IfNotExistsExpression struct {
	Path  UpdateActionPath
	Value SetActionValue
}

func (ine *IfNotExistsExpression) setActionFunctionNode() {}
func (ine *IfNotExistsExpression) setActionOperandNode()  {}
func (ine *IfNotExistsExpression) setActionValue()        {}
func (ine *IfNotExistsExpression) String() string {
	var out bytes.Buffer
	out.WriteString("if_not_exists(")
	out.WriteString(ine.Path.String())
	out.WriteString(", ")
	out.WriteString(ine.Value.String())
	out.WriteString(")")
	return out.String()
}

type ListAppendExpression struct {
	Target PathOperand
	Source PathOperand
}

func (exp *ListAppendExpression) setActionFunctionNode() {}
func (exp *ListAppendExpression) setActionOperandNode()  {}
func (exp *ListAppendExpression) setActionValue()        {}
func (exp *ListAppendExpression) String() string {
	var out bytes.Buffer
	out.WriteString("list_append(")
	out.WriteString(exp.Target.String())
	out.WriteString(", ")
	out.WriteString(exp.Source.String())
	out.WriteString(")")
	return out.String()
}

type SetAction struct {
	Path  UpdateActionPath
	Value SetActionValue
}

type RemoveClause struct {
	Paths []UpdateActionPath
}

func (exp *RemoveClause) String() string {
	var out bytes.Buffer
	out.WriteString("REMOVE ")
	for i, path := range exp.Paths {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(path.String())
	}
	return out.String()
}

type AddClause struct {
	Actions []*AddAction
}

func (exp *AddClause) String() string {
	var out bytes.Buffer
	out.WriteString("ADD ")
	for i, action := range exp.Actions {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(action.String())
	}
	return out.String()
}

type AddAction struct {
	Path  *AttributeNameOperand
	Value UpdateActionPath
}

func (exp *AddAction) String() string {
	var out bytes.Buffer
	out.WriteString(exp.Path.String())
	out.WriteString(" ")
	out.WriteString(exp.Value.String())
	return out.String()
}

type DeleteClause struct {
	Actions []*DeleteAction
}

func (exp *DeleteClause) String() string {
	var out bytes.Buffer
	out.WriteString("DELETE ")
	for i, action := range exp.Actions {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(action.String())
	}
	return out.String()
}

type DeleteAction struct {
	Path   *AttributeNameOperand
	Subset *AttributeNameOperand
}

func (exp *DeleteAction) String() string {
	var out bytes.Buffer
	out.WriteString(exp.Path.String())
	out.WriteString(" ")
	out.WriteString(exp.Subset.String())
	return out.String()
}
