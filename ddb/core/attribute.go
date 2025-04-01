package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"math"
	"strconv"
	"strings"
)

type AttributeValue struct {
	B    *[]byte `json:",omitempty"`
	Bool *bool   `json:",omitempty"`
	// BS
	L    *[]AttributeValue          `json:",omitempty"`
	M    *map[string]AttributeValue `json:",omitempty"`
	N    *string                    `json:",omitempty"`
	NS   *[]string                  `json:",omitempty"`
	NULL *bool                      `json:",omitempty"`
	S    *string                    `json:",omitempty"`
	SS   *[]string                  `json:",omitempty"`
}

func (a AttributeValue) Type() string {
	if a.B != nil {
		return "B"
	} else if a.Bool != nil {
		return "BOOL"
	} else if a.L != nil {
		return "L"
	} else if a.M != nil {
		return "M"
	} else if a.N != nil {
		return "N"
	} else if a.NS != nil {
		return "NS"
	} else if a.NULL != nil {
		return "NULL"
	} else if a.S != nil {
		return "S"
	} else if a.SS != nil {
		return "SS"
	}

	panic("unreachable")
}

func (a AttributeValue) Bytes() []byte {
	if a.B != nil {
		return *a.B
	} else if a.Bool != nil {
		if *a.Bool {
			return []byte{1}
		} else {
			return []byte{0}
		}
	} else if a.L != nil {
		panic("can't convert L to bytes")
	} else if a.M != nil {
		panic("can't convert M to bytes")
	} else if a.N != nil {
		return []byte(fmt.Sprintf("%s", *a.N))
	} else if a.NS != nil {
		panic("can't convert NS to bytes")
	} else if a.NULL != nil {
		panic("can't convert NULL to bytes")
	} else if a.S != nil {
		return []byte(*a.S)
	} else if a.SS != nil {
		panic("can't convert SS to bytes")
	}

	panic("unreachable")
}

func (a AttributeValue) String() string {
	if a.B != nil {
		return fmt.Sprintf("B=%s", *a.B)
	} else if a.Bool != nil {
		return fmt.Sprintf("Bool=%t", *a.Bool)
	} else if a.L != nil {
		var b strings.Builder
		b.WriteString("L=[")
		for _, v := range *a.L {
			b.WriteString(v.String())
			b.WriteString(",")
		}
		b.WriteString("]")
		return b.String()
	} else if a.M != nil {
		var b strings.Builder
		b.WriteString("M={")
		for k, v := range *a.M {
			b.WriteString(fmt.Sprintf("%s:%s", k, v))
			b.WriteString(",")
		}
		b.WriteString("}")
		return b.String()
	} else if a.N != nil {
		return fmt.Sprintf("N=%s", *a.N)
	} else if a.NS != nil {
		var b strings.Builder
		b.WriteString("NS=[")
		for _, v := range *a.NS {
			b.WriteString(v)
			b.WriteString(",")
		}
		b.WriteString("]")
		return b.String()
	} else if a.NULL != nil {
		return fmt.Sprintf("NULL=%t", *a.NULL)
	} else if a.S != nil {
		return fmt.Sprintf("S=%s", *a.S)
	} else if a.SS != nil {
		var b strings.Builder
		b.WriteString("SS=[")
		for _, v := range *a.SS {
			b.WriteString(v)
			b.WriteString(",")
		}
		b.WriteString("]")
		return b.String()
	}

	panic("unreachable")
}

func (a AttributeValue) Compare(other AttributeValue) (int, error) {
	if a.B != nil {
		if other.B == nil {
			return -1, errors.New("B is nil")
		}

		return bytes.Compare(*a.B, *other.B), nil
	} else if a.Bool != nil {
		if other.Bool == nil {
			return -1, errors.New("B is nil")
		}

		if *a.Bool == *other.Bool {
			return 0, nil
		} else if *a.Bool {
			return 1, nil
		} else {
			return -1, nil
		}
	} else if a.L != nil {
		return -1, errors.New("can't compare L")
	} else if a.M != nil {
		return -1, errors.New("can't compare M")
	} else if a.N != nil {
		if other.N == nil {
			return -1, errors.New("B is nil")
		}

		// TODO: use something like BigDecimal later!!
		numA, err := strconv.ParseFloat(*a.N, 64)
		if err != nil {
			return -1, err
		}
		numOther, err := strconv.ParseFloat(*other.N, 64)
		if err != nil {
			return -1, err
		}
		epsilon := 0.0001
		if math.Abs(numA-numOther) < epsilon {
			// numA and numOther are approximately equal
			return 0, nil
		} else if numA > numOther {
			return 1, nil
		} else {
			return -1, nil
		}
	} else if a.NS != nil {
		return -1, errors.New("can't compare NS")
	} else if a.NULL != nil {
		return -1, errors.New("can't compare NULL")
	} else if a.S != nil {
		if other.S == nil {
			return -1, errors.New("B is nil")
		}

		return strings.Compare(*a.S, *other.S), nil
	} else if a.SS != nil {
		if other.S == nil {
			return -1, errors.New("B is nil")
		}
	}

	panic("unreachable")
}

func (a AttributeValue) Equal(other AttributeValue) bool {
	if a.B != nil {
		if other.B == nil {
			return false
		}

		return bytes.Compare(*a.B, *other.B) == 0
	} else if a.Bool != nil {
		if other.Bool == nil {
			return false
		}

		return *a.Bool == *other.Bool
	} else if a.L != nil {
		if other.L == nil {
			return false
		}
		if len(*a.L) != len(*other.L) {
			return false
		}

		for i, v := range *a.L {
			if !v.Equal((*other.L)[i]) {
				return false
			}
		}
		return true
	} else if a.M != nil {
		if other.M == nil {
			return false
		}
		if len(*a.M) != len(*other.M) {
			return false
		}

		for k, v := range *a.M {
			v2, ok := (*other.M)[k]
			if !ok {
				return false
			}
			if !v.Equal(v2) {
				return false
			}
		}
		return true
	} else if a.N != nil {
		if other.N == nil {
			return false
		}
		return *a.N == *other.N
	} else if a.NS != nil {
		if other.NS == nil {
			return false
		}
		for i, v := range *a.NS {
			v2 := (*other.NS)[i]
			if v != v2 {
				return false
			}
		}
		return true
	} else if a.NULL != nil {
		if other.NULL == nil {
			return false
		}

		return *a.NULL == *other.NULL
	} else if a.S != nil {
		if other.S == nil {
			return false
		}
		return *a.S == *other.S
	} else if a.SS != nil {
		if other.SS == nil {
			return false
		}
		for i, v := range *a.SS {
			v2 := (*other.SS)[i]
			if v != v2 {
				return false
			}
		}
		return true
	}

	panic("unreachable")
}

func (a AttributeValue) Clone() AttributeValue {
	clonedVal := AttributeValue{}

	if a.B != nil {
		b := make([]byte, len(*a.B))
		copy(b, *a.B)
		clonedVal.B = &b
	} else if a.Bool != nil {
		b := *a.Bool
		clonedVal.Bool = &b
	} else if a.L != nil {
		l := make([]AttributeValue, len(*a.L))
		for i, v := range *a.L {
			l[i] = v.Clone()
		}
		clonedVal.L = &l
	} else if a.M != nil {
		m := make(map[string]AttributeValue)
		for k, v := range *a.M {
			m[k] = v.Clone()
		}
		clonedVal.M = &m
	} else if a.N != nil {
		n := *a.N
		clonedVal.N = &n
	} else if a.NS != nil {
		ns := make([]string, len(*a.NS))
		copy(ns, *a.NS)
		clonedVal.NS = &ns
	} else if a.NULL != nil {
		null := *a.NULL
		clonedVal.NULL = &null
	} else if a.S != nil {
		s := *a.S
		clonedVal.S = &s
	} else if a.SS != nil {
		ss := make([]string, len(*a.SS))
		copy(ss, *a.SS)
		clonedVal.SS = &ss
	} else {
		panic("unreachable")
	}
	return clonedVal
}

func (a AttributeValue) ToDdbAttributeValue() types.AttributeValue {
	if a.B != nil {
		return &types.AttributeValueMemberB{Value: *a.B}
	} else if a.Bool != nil {
		return &types.AttributeValueMemberBOOL{Value: *a.Bool}
	} else if a.L != nil {
		vals := make([]types.AttributeValue, len(*a.L))
		for i, v := range *a.L {
			vals[i] = v.ToDdbAttributeValue()
		}
		return &types.AttributeValueMemberL{Value: vals}
	} else if a.M != nil {
		vals := make(map[string]types.AttributeValue)
		for k, v := range *a.M {
			vals[k] = v.ToDdbAttributeValue()
		}
		return &types.AttributeValueMemberM{Value: vals}
	} else if a.N != nil {
		return &types.AttributeValueMemberN{Value: *a.N}
	} else if a.NS != nil {
		return &types.AttributeValueMemberNS{Value: *a.NS}
	} else if a.NULL != nil {
		return &types.AttributeValueMemberNULL{Value: *a.NULL}
	} else if a.S != nil {
		return &types.AttributeValueMemberS{Value: *a.S}
	} else if a.SS != nil {
		return &types.AttributeValueMemberSS{Value: *a.SS}
	}

	panic("unreachable")
}

func EncodingAttributeValue(m map[string]AttributeValue) ([]byte, error) {
	bs, err := json.Marshal(m)

	return bs, err
}

func DecodingAttributeValues(bs []byte) (map[string]AttributeValue, error) {
	var m map[string]AttributeValue
	err := json.Unmarshal(bs, &m)

	return m, err
}

func TransformDdbAttributeValue(val types.AttributeValue) AttributeValue {
	switch val.(type) {
	case *types.AttributeValueMemberB:
		b := val.(*types.AttributeValueMemberB)
		return AttributeValue{
			B: &b.Value,
		}
	case *types.AttributeValueMemberBOOL:
		b := val.(*types.AttributeValueMemberBOOL)
		return AttributeValue{
			Bool: &b.Value,
		}
	case *types.AttributeValueMemberL:
		l := val.(*types.AttributeValueMemberL)
		list := make([]AttributeValue, len(l.Value))
		for i, v := range l.Value {
			list[i] = TransformDdbAttributeValue(v)
		}
		return AttributeValue{
			L: &list,
		}
	case *types.AttributeValueMemberM:
		m := val.(*types.AttributeValueMemberM)
		m2 := make(map[string]AttributeValue)
		for k, v := range m.Value {
			m2[k] = TransformDdbAttributeValue(v)
		}
		return AttributeValue{
			M: &m2,
		}
	case *types.AttributeValueMemberN:
		n := val.(*types.AttributeValueMemberN)
		return AttributeValue{
			N: &n.Value,
		}
	case *types.AttributeValueMemberNS:
		ns := val.(*types.AttributeValueMemberNS)
		return AttributeValue{
			NS: &ns.Value,
		}
	case *types.AttributeValueMemberNULL:
		n := val.(*types.AttributeValueMemberNULL)
		return AttributeValue{
			NULL: &n.Value,
		}
	case *types.AttributeValueMemberS:
		s := val.(*types.AttributeValueMemberS)
		return AttributeValue{
			S: &s.Value,
		}
	case *types.AttributeValueMemberSS:
		ss := val.(*types.AttributeValueMemberSS)
		return AttributeValue{
			SS: &ss.Value,
		}
	default:
		panic("unknown attribute type")
	}
}

func NewEntryFromItem(m map[string]types.AttributeValue) *Entry {
	body := make(map[string]AttributeValue)
	for key, val := range m {
		body[key] = TransformDdbAttributeValue(val)
	}
	return &Entry{
		Body: body,
	}
}

func NewItemFromEntry(m map[string]AttributeValue) map[string]types.AttributeValue {
	m2 := make(map[string]types.AttributeValue)
	for key, val := range m {
		m2[key] = val.ToDdbAttributeValue()
	}

	return m2
}
