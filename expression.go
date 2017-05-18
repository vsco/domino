package domino

import (
	"fmt"
	"regexp"
	"strings"
)

/*Expression represents a dynamo Condition expression, i.e. And(if_empty(...), size(path) >0) */
type Expression interface {
	construct(uint, bool) (string, map[string]interface{}, uint)
}
type ExpressionGroup struct {
	expressions []Expression
	op          string
}

type Condition struct {
	exprF func([]string) string
	args  []interface{}
}

type KeyCondition struct {
	Condition
}

type negation struct {
	expression Expression
}

const (
	eq  = "="
	neq = "<>"
	lt  = "<"
	lte = "<="
	gt  = ">"
	gte = ">="
)

var nonalpha *regexp.Regexp = regexp.MustCompile("[^a-zA-Z_0-9]")

func generatePlaceholder(a interface{}, counter uint) string {
	r := fmt.Sprintf("%v_%v", a, counter)
	return ":" + nonalpha.ReplaceAllString(r, "_")
}

/*********************************************************************************/
/******************************** ExpressionGroups *******************************/
/*********************************************************************************/
/*Groups expression by AND and OR operators, i.e. <expr> OR <expr>*/

func (e ExpressionGroup) construct(counter uint, topLevel bool) (string, map[string]interface{}, uint) {
	a := e.expressions
	m := make(map[string]interface{})
	var r string

	for i := 0; i < len(a); i++ {
		if i > 0 {
			r += " " + e.op + " "
		}
		substring, placeholders, newCounter := a[i].construct(counter, false)
		r += substring
		for k, v := range placeholders {
			m[k] = v
		}

		counter = newCounter
	}

	if !topLevel && len(a) > 1 {
		r = fmt.Sprintf("(%v)", r)
	}

	return r, m, counter
}

/*Or represents a dynamo OR expression. All expressions are or'd together*/
func Or(c ...Expression) ExpressionGroup {
	return ExpressionGroup{
		c,
		"OR",
	}
}

/*And represents a dynamo AND expression. All expressions are and'd together*/
func And(c ...Expression) ExpressionGroup {
	return ExpressionGroup{
		c,
		"AND",
	}
}

/*String stringifies expressions for easy debugging*/
func (c ExpressionGroup) String() string {
	s, _, _ := c.construct(0, true)
	return s
}

/*********************************************************************************/
/******************************** Negation Expression ****************************/
/*********************************************************************************/

func (n negation) construct(counter uint, topLevel bool) (string, map[string]interface{}, uint) {
	s, m, c := n.expression.construct(counter, topLevel)
	r := "NOT " + s
	if !topLevel {
		r = fmt.Sprintf("(%v)", r)
	}

	return r, m, c
}

func (c negation) String() string {
	s, _, _ := c.construct(0, true)
	return s
}

/*Not represents the dynamo NOT operator*/
func Not(c Expression) negation {
	return negation{c}
}

/*********************************************************************************/
/******************************** Conditions *************************************/
/*********************************************************************************/
/*Conditions that only apply to keys*/

func (c Condition) construct(counter uint, topLevel bool) (string, map[string]interface{}, uint) {
	a := make([]string, len(c.args))
	m := make(map[string]interface{})
	for i, b := range c.args {
		a[i] = generatePlaceholder(b, counter)
		m[a[i]] = b
		counter++
	}
	s := c.exprF(a)
	return s, m, counter
}

func (c Condition) String() string {
	s, _, _ := c.construct(0, true)
	return s
}

/*In represents the dynamo 'in' operator*/
func (p *DynamoField) In(elems ...interface{}) Condition {
	return Condition{
		exprF: func(placeholders []string) string {
			return fmt.Sprintf("(%s in (%s))", p.name, strings.Join(placeholders, ","))
		},
		args: elems,
	}

}

/*Exists represents the dynamo attribute_exists operator*/
func (p *DynamoField) Exists() Condition {
	return Condition{
		exprF: func(placeholders []string) string {
			return "attribute_exists(" + p.name + ")"
		},
	}
}

/*NotExists represents the dynamo attribute_not_exists operator*/
func (p *DynamoField) NotExists() Condition {
	return Condition{
		exprF: func(placeholders []string) string {
			return "attribute_not_exists(" + p.name + ")"
		},
	}
}

/*Contains represents the dynamo contains operator*/
func (p *DynamoField) Contains(a interface{}) Condition {
	return Condition{
		exprF: func(placeholders []string) string {
			return fmt.Sprintf("contains("+p.name+",%v)", placeholders[0])
		},
		args: []interface{}{a},
	}
}

/*Contains represents the dynamo contains size*/
func (p *DynamoField) Size(op string, a interface{}) Condition {
	return Condition{
		exprF: func(placeholders []string) string {
			return fmt.Sprintf("size("+p.name+") "+op+"%v", placeholders[0])
		},
		args: []interface{}{a},
	}
}

/*********************************************************************************/
/******************************** Key Conditions *********************************/
/*********************************************************************************/

func (p *DynamoField) operation(op string, a interface{}) KeyCondition {
	return KeyCondition{
		Condition{
			exprF: func(placeholders []string) string {
				return fmt.Sprintf("%s %s %v", p.name, op, placeholders[0])
			},
			args: []interface{}{a},
		},
	}
}

func (p *DynamoField) Equals(a interface{}) KeyCondition {
	return p.operation(eq, a)
}
func (p *DynamoField) NotEquals(a interface{}) KeyCondition {
	return p.operation(neq, a)
}
func (p *DynamoField) LessThan(a interface{}) KeyCondition {
	return p.operation(lt, a)
}
func (p *DynamoField) LessThanOrEq(a interface{}) KeyCondition {
	return p.operation(lte, a)
}
func (p *DynamoField) GreaterThan(a interface{}) KeyCondition {
	return p.operation(gt, a)
}
func (p *DynamoField) GreaterThanOrEq(a interface{}) KeyCondition {
	return p.operation(gte, a)
}

func (p *DynamoField) BeginsWith(a interface{}) KeyCondition {
	return KeyCondition{
		Condition{
			exprF: func(placeholders []string) string {
				return fmt.Sprintf("begins_with("+p.name+",%v)", placeholders[0])
			},
			args: []interface{}{a},
		},
	}
}

func (p *DynamoField) Between(a interface{}, b interface{}) KeyCondition {
	return KeyCondition{
		Condition{
			exprF: func(placeholders []string) string {
				return fmt.Sprintf("("+p.name+" between %v and %v)", placeholders[0], placeholders[1])
			},
			args: []interface{}{a, b},
		},
	}
}

/*********************************************************************************/
/******************************** Update Expressions *****************************/
/*********************************************************************************/
type UpdateExpression struct {
	op string
	f  func(counter uint) (string, map[string]interface{}, uint)
}

/*SetField sets a dynamo Field. Set onlyIfEmpty to true if you want to prevent overwrites*/
func (Field *DynamoField) SetField(a interface{}, onlyIfEmpty bool) *UpdateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		ph := generatePlaceholder(a, c)
		r := ph
		if onlyIfEmpty {
			r = fmt.Sprintf("if_not_exists(%v,%v)", Field.name, ph)
		}
		s := Field.name + " = " + r
		m := map[string]interface{}{
			ph: a,
		}
		c++
		return s, m, c
	}
	return &UpdateExpression{op: "SET", f: f}
}

/*Add adds an amount to dynamo numeric Field*/
func (Field *Numeric) Add(amount float64) *UpdateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		ph := generatePlaceholder(amount, c)
		s := Field.name + " " + ph
		m := map[string]interface{}{ph: amount}
		c++
		return s, m, c
	}
	return &UpdateExpression{op: "ADD", f: f}
}

/*Append appends an element to a list Field*/
func (Field *dynamoCollectionField) Append(a interface{}) *UpdateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		ph := generatePlaceholder(a, c)
		s := fmt.Sprintf(Field.name+" = list_append(%v,"+Field.name+")", ph)
		// s := Field.name + " " + ph
		m := map[string]interface{}{ph: []interface{}{a}}
		c++
		return s, m, c
	}
	return &UpdateExpression{op: "SET", f: f}
}

/*RemoveKey removes an element from a map Field*/
func (Field *Map) RemoveKey(s string) *UpdateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		c++
		m := make(map[string]interface{})
		return s, m, c
	}
	return &UpdateExpression{op: "REMOVE", f: f}
}

/*RemoveElemIndex removes an element from collection Field index*/
func (Field *dynamoCollectionField) RemoveElemIndex(idx uint) *UpdateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		c++
		s := fmt.Sprintf("%v[%v]", Field.name, idx)
		m := make(map[string]interface{})
		return s, m, c
	}
	return &UpdateExpression{op: "REMOVE", f: f}
}

/*Increment a numeric counter Field*/
func (Field *Numeric) Increment(by uint) *UpdateExpression {
	return Field.Add(float64(by))
}

/*Decrement a numeric counter Field*/
func (Field *Numeric) Decrement(by uint) *UpdateExpression {
	return Field.Add(-float64(by))
}
