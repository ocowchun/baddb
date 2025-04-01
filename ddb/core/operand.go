package core

import "fmt"

type Operand interface {
	operand()
}

type PathOperand interface {
	operand()
	pathOperand()
	String() string
}

type AttributeNameOperand struct {
	Name string
}

func (a *AttributeNameOperand) operand()     {}
func (a *AttributeNameOperand) pathOperand() {}
func (a *AttributeNameOperand) String() string {
	return a.Name
}

type IndexOperand struct {
	Left  PathOperand
	Index int
}

func (i *IndexOperand) operand()     {}
func (i *IndexOperand) pathOperand() {}
func (i *IndexOperand) String() string {
	return fmt.Sprintf("%s[%d]", i.Left, i.Index)
}

type DotOperand struct {
	Left  PathOperand
	Right PathOperand
}

func (d *DotOperand) operand()     {}
func (d *DotOperand) pathOperand() {}
func (d *DotOperand) String() string {
	return fmt.Sprintf("%s.%s", d.Left, d.Right)
}
