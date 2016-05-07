package domino

import (
	"strconv"
	"strings"
)

type Path string
type Type string

const (
	S    = "S"
	SS   = "SS"
	NN   = "N"
	NS   = "NS"
	B    = "B"
	BS   = "BS"
	BOOL = "Bool"
	NULL = "Null"
	L    = "List"
	M    = "M"
)

const (
	eq  = "="
	neq = "<>"
	lt  = "<"
	lte = "<="
	gt  = ">"
	gte = ">="
)

type Expression interface {
	toString() string
	attributeValues() map[string]string //Map from placeholder to value, i.e. ":p" -> ""
}

type condition struct {
	expr         string
	placeHolders []string
}

/*Groups expression by AND and OR operators, i.e. <expr> OR <expr>*/
type expressionGroup struct {
	expressions []Expression
	op          string
}

func (c condition) toString() string {
	return string(c.expr)
}
func (c condition) attributeValues() (m map[string]string) {
	m = make(map[string]string)
	for _, r := range c.placeHolders {
		m[":"+r] = r
	}
	return m
}
func (c condition) String() string {
	return c.toString()
}
func (e expressionGroup) toString() (r string) {
	a := e.expressions
	r = "("
	for i := 0; i < len(a); i++ {
		if i > 0 {
			r += " " + e.op + " "
		}
		r += (a[i]).toString()
	}
	r += ")"
	return
}

func (c expressionGroup) attributeValues() (m map[string]string) {
	m = make(map[string]string)
	for _, e := range c.expressions {
		for k, v := range e.attributeValues() {
			m[k] = v
		}
	}
	return
}
func (c expressionGroup) String() string {
	return c.toString()
}

func Or(c ...Expression) expressionGroup {
	return expressionGroup{
		c,
		"OR",
	}
}
func And(c ...Expression) expressionGroup {
	return expressionGroup{
		c,
		"AND",
	}
}

func Not(e Expression) condition {
	return condition{
		expr: "(NOT " + e.toString() + ")",
	}
}

func (p Path) operation(op string, a string) condition {
	return condition{
		expr:         "(" + string(p) + " " + op + " :" + a + ")",
		placeHolders: []string{a},
	}
}

func (p Path) Between(a string, b string) condition {
	return condition{
		expr:         "(" + string(p) + " between(:" + a + ", :" + b + "))",
		placeHolders: []string{a, b},
	}
}

func (p Path) In(elems ...string) condition {
	return condition{
		expr: string(p) + " in (" + strings.Join(elems, ",") + ")",
	}
}

func (p Path) Equals(a string) condition {
	return p.operation(eq, a)
}
func (p Path) NotEquals(a string) condition {
	return p.operation(neq, a)
}
func (p Path) LessThan(a string) condition {
	return p.operation(lt, a)
}
func (p Path) LessThanOrEq(a string) condition {
	return p.operation(lte, a)
}
func (p Path) GreaterThan(a string) condition {
	return p.operation(gt, a)
}
func (p Path) GreaterThanOrEq(a string) condition {
	return p.operation(gte, a)
}

func (p Path) Exists() condition {
	return condition{
		expr: "attribute_exists(" + string(p) + ")",
	}
}

func (p Path) NotExists() condition {
	return condition{
		expr: "attribute_not_exists(" + string(p) + ")",
	}
}

func (p Path) BeginsWith(a string) condition {
	return condition{
		expr:         "begins_with(" + string(p) + ",:" + a + ")",
		placeHolders: []string{a},
	}
}

func (p Path) Contains(a string) condition {
	return condition{
		expr:         "contains(" + string(p) + ",:" + a + ")",
		placeHolders: []string{a},
	}
}

func (p Path) Size(op string, value int) condition {
	a := strconv.Itoa(value)
	return condition{
		expr:         "size(" + string(p) + ") " + op + " :" + a,
		placeHolders: []string{a},
	}
}
