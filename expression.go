package domino

import (
	"fmt"
	"regexp"
	"strings"
)

/*Expression represents a dynamo condition expression, i.e. And(if_empty(...), size(path) >0) */
type Expression interface {
	construct(uint, bool) (string, map[string]interface{}, uint)
}
type expressionGroup struct {
	expressions []Expression
	op          string
}

type condition struct {
	exprF func([]string) string
	args  []interface{}
}

type keyCondition struct {
	condition
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

func (e expressionGroup) construct(counter uint, topLevel bool) (string, map[string]interface{}, uint) {
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
func Or(c ...Expression) expressionGroup {
	return expressionGroup{
		c,
		"OR",
	}
}

/*And represents a dynamo AND expression. All expressions are and'd together*/
func And(c ...Expression) expressionGroup {
	return expressionGroup{
		c,
		"AND",
	}
}

/*String stringifies expressions for easy debugging*/
func (c expressionGroup) String() string {
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

func (c condition) construct(counter uint, topLevel bool) (string, map[string]interface{}, uint) {
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

func (c condition) String() string {
	s, _, _ := c.construct(0, true)
	return s
}

/*In represents the dynamo 'in' operator*/
func (p *dynamoField) In(elems ...interface{}) condition {
	return condition{
		exprF: func(placeholders []string) string {
			return fmt.Sprintf("(%s in (%s))", p.name, strings.Join(placeholders, ","))
		},
		args: elems,
	}

}

/*Exists represents the dynamo attribute_exists operator*/
func (p *dynamoField) Exists() condition {
	return condition{
		exprF: func(placeholders []string) string {
			return "attribute_exists(" + p.name + ")"
		},
	}
}

/*NotExists represents the dynamo attribute_not_exists operator*/
func (p *dynamoField) NotExists() condition {
	return condition{
		exprF: func(placeholders []string) string {
			return "attribute_not_exists(" + p.name + ")"
		},
	}
}

/*Contains represents the dynamo contains operator*/
func (p *dynamoField) Contains(a interface{}) condition {
	return condition{
		exprF: func(placeholders []string) string {
			return fmt.Sprintf("contains("+p.name+",%v)", placeholders[0])
		},
		args: []interface{}{a},
	}
}

/*Contains represents the dynamo contains size*/
func (p *dynamoField) Size(op string, a interface{}) condition {
	return condition{
		exprF: func(placeholders []string) string {
			return fmt.Sprintf("size("+p.name+") "+op+"%v", placeholders[0])
		},
		args: []interface{}{a},
	}
}

/*********************************************************************************/
/******************************** Key Conditions *********************************/
/*********************************************************************************/

func (p *dynamoField) operation(op string, a interface{}) keyCondition {
	return keyCondition{
		condition{
			exprF: func(placeholders []string) string {
				return fmt.Sprintf("%s %s %v", p.name, op, placeholders[0])
			},
			args: []interface{}{a},
		},
	}
}

func (p *dynamoField) Equals(a interface{}) keyCondition {
	return p.operation(eq, a)
}
func (p *dynamoField) NotEquals(a interface{}) keyCondition {
	return p.operation(neq, a)
}
func (p *dynamoField) LessThan(a interface{}) keyCondition {
	return p.operation(lt, a)
}
func (p *dynamoField) LessThanOrEq(a interface{}) keyCondition {
	return p.operation(lte, a)
}
func (p *dynamoField) GreaterThan(a interface{}) keyCondition {
	return p.operation(gt, a)
}
func (p *dynamoField) GreaterThanOrEq(a interface{}) keyCondition {
	return p.operation(gte, a)
}

func (p *dynamoField) BeginsWith(a interface{}) keyCondition {
	return keyCondition{
		condition{
			exprF: func(placeholders []string) string {
				return fmt.Sprintf("begins_with("+p.name+",%v)", placeholders[0])
			},
			args: []interface{}{a},
		},
	}
}

func (p *dynamoField) Between(a interface{}, b interface{}) keyCondition {
	return keyCondition{
		condition{
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
type updateExpression struct {
	op string
	f  func(counter uint) (string, map[string]interface{}, uint)
}

/*SetField sets a dynamo field. Set onlyIfEmpty to true if you want to prevent overwrites*/
func (field *dynamoField) SetField(a interface{}, onlyIfEmpty bool) *updateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		ph := generatePlaceholder(a, c)
		r := ph
		if onlyIfEmpty {
			r = fmt.Sprintf("if_not_exists(%v,%v)", field.name, ph)
		}
		s := field.name + " = " + r
		m := map[string]interface{}{
			ph: a,
		}
		c++
		return s, m, c
	}
	return &updateExpression{op: "SET", f: f}
}

/*Add adds an amount to dynamo numeric field*/
func (field *Numeric) Add(amount float64) *updateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		ph := generatePlaceholder(amount, c)
		s := field.name + " " + ph
		m := map[string]interface{}{ph: amount}
		c++
		return s, m, c
	}
	return &updateExpression{op: "ADD", f: f}
}

/*Append appends an element to a list field*/
func (field *dynamoCollectionField) Append(a interface{}) *updateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		ph := generatePlaceholder(a, c)
		s := fmt.Sprintf(field.name+" = list_append(%v,"+field.name+")", ph)
		// s := field.name + " " + ph
		m := map[string]interface{}{ph: []interface{}{a}}
		c++
		return s, m, c
	}
	return &updateExpression{op: "SET", f: f}
}

/*RemoveKey removes an element from a map field*/
func (field *Map) RemoveKey(s string) *updateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		c++
		m := make(map[string]interface{})
		return s, m, c
	}
	return &updateExpression{op: "REMOVE", f: f}
}

/*RemoveElemIndex removes an element from collection field index*/
func (field *dynamoCollectionField) RemoveElemIndex(idx uint) *updateExpression {
	f := func(c uint) (string, map[string]interface{}, uint) {
		c++
		s := fmt.Sprintf("%v[%v]", field.name, idx)
		m := make(map[string]interface{})
		return s, m, c
	}
	return &updateExpression{op: "REMOVE", f: f}
}

/*Increment a numeric counter field*/
func (field *Numeric) Increment(by uint) *updateExpression {
	return field.Add(float64(by))
}

/*Decrement a numeric counter field*/
func (field *Numeric) Decrement(by uint) *updateExpression {
	return field.Add(-float64(by))
}
