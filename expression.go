package domino

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

/*Expression represents a dynamo Condition expression, i.e. And(if_empty(...), size(path) >0) */
type Expression interface {
	construct(counter uint, b bool) (string, map[string]*string, map[string]interface{}, uint)
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

func generateNamePlaceholder(a interface{}, counter uint) string {
	r := fmt.Sprintf("%v_%v", a, counter)
	return "#" + nonalpha.ReplaceAllString(r, "_")
}

/*********************************************************************************/
/******************************** ExpressionGroups *******************************/
/*********************************************************************************/
/*Groups expression by AND and OR operators, i.e. <expr> OR <expr>*/

func (e ExpressionGroup) construct(counter uint, topLevel bool) (expr string, exprNames map[string]*string, exprValues map[string]interface{}, c uint) {
	a := e.expressions

	for i := 0; i < len(a); i++ {
		if i > 0 {
			expr += " " + e.op + " "
		}
		substring, names, placeholders, newCounter := a[i].construct(counter, false)
		expr += substring
		if exprValues == nil && len(placeholders) > 0 {
			exprValues = placeholders
		} else {
			for k, v := range placeholders {
				exprValues[k] = v
			}
		}
		if exprNames == nil && len(names) > 0 {
			exprNames = names
		} else {
			for k, v := range names {
				exprNames[k] = v
			}
		}

		counter = newCounter
	}

	if !topLevel && len(a) > 1 {
		expr = fmt.Sprintf("(%v)", expr)
	}
	c = counter
	return
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
	s, _, _, _ := c.construct(0, true)
	return s
}

/*********************************************************************************/
/******************************** Negation Expression ****************************/
/*********************************************************************************/

func (n negation) construct(counter uint, topLevel bool) (string, map[string]*string, map[string]interface{}, uint) {
	s, names, m, c := n.expression.construct(counter, topLevel)
	r := "NOT " + s
	if !topLevel {
		r = fmt.Sprintf("(%v)", r)
	}

	return r, names, m, c
}

func (c negation) String() string {
	s, _, _, _ := c.construct(0, true)
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

func (c Condition) construct(counter uint, topLevel bool) (string, map[string]*string, map[string]interface{}, uint) {
	a := make([]string, len(c.args))
	var m map[string]interface{}
	for i, b := range c.args {
		a[i] = generatePlaceholder(b, counter)
		if m == nil {
			m = map[string]interface{}{}
		}
		m[a[i]] = b
		counter++
	}
	s := c.exprF(a)
	return s, nil, m, counter
}

func (c Condition) String() string {
	s, _, _, _ := c.construct(0, true)
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
	f  func(counter uint) (expression string, exprAttributeNames map[string]*string, exprAttributeValues map[string]interface{}, c uint)
}

/*SetField sets a dynamo Field. Set onlyIfEmpty to true if you want to prevent overwrites*/
func (Field *DynamoField) SetField(a interface{}, onlyIfEmpty bool) *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
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
		return s, nil, m, c
	}
	return &UpdateExpression{op: "SET", f: f}
}

/*RemoveField removes a dynamo Field.*/
func (Field *DynamoField) RemoveField() *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
		c++
		return Field.name, nil, nil, c
	}
	return &UpdateExpression{op: "REMOVE", f: f}
}

/*Add adds an amount to dynamo numeric Field*/
func (Field *Numeric) Add(amount float64) *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
		ph := generatePlaceholder(amount, c)
		s := Field.name + " " + ph
		m := map[string]interface{}{ph: amount}
		c++
		return s, nil, m, c
	}
	return &UpdateExpression{op: "ADD", f: f}
}

/*Append appends an element to a list Field*/
func (Field *dynamoListField) Append(a interface{}) *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
		ph := generatePlaceholder(a, c)
		s := fmt.Sprintf(Field.name+" = list_append(%v,"+Field.name+")", ph)
		m := map[string]interface{}{ph: []interface{}{a}}
		c++
		return s, nil, m, c
	}
	return &UpdateExpression{op: "SET", f: f}
}

func (Field *dynamoListField) Set(index int, a interface{}) *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
		ph := generatePlaceholder(a, c)
		s := fmt.Sprintf(Field.name+"[%v] = %v", index, ph)
		m := map[string]interface{}{ph: []interface{}{a}}
		c++
		return s, nil, m, c
	}
	return &UpdateExpression{op: "SET", f: f}
}

func (Field *dynamoListField) Remove(index int) *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
		s := fmt.Sprintf("%v[%v]", Field.name, index)
		return s, nil, nil, c
	}
	return &UpdateExpression{op: "REMOVE", f: f}
}

func (Field *dynamoMapField) Set(key string, a interface{}) *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
		ph := generatePlaceholder(key, c)
		s := fmt.Sprintf("%v.%v = %v", Field.name, key, ph)
		m := map[string]interface{}{
			ph: a,
		}
		c++
		return s, nil, m, c
	}
	return &UpdateExpression{op: "SET", f: f}
}

/*RemoveKey removes an element from a map Field*/
func (Field *dynamoMapField) Remove(key string) *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
		// fn := generateNamePlaceholder(Field.name, c)
		// c++
		// fv := generateNamePlaceholder(key, c)
		s := fmt.Sprintf("%v.%v", Field.name, key)
		c++
		// n := map[string]*string{fn: &Field.name, fv: &key}

		return s, nil, nil, c
	}
	return &UpdateExpression{op: "REMOVE", f: f}
}

func (Field *dynamoSetField) Add(a *dynamodb.AttributeValue) *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
		ph := generatePlaceholder(c, c)
		s := fmt.Sprintf(Field.name+" %v", ph)
		m := map[string]interface{}{ph: a}

		c++
		return s, nil, m, c
	}
	return &UpdateExpression{op: "ADD", f: f}
}

func (Field *dynamoSetField) AddFloat(a float64) *UpdateExpression {
	v := strconv.FormatFloat(a, 'E', -1, 64)
	attr := &dynamodb.AttributeValue{
		NS: []*string{&v},
	}
	return Field.Add(attr)
}
func (Field *dynamoSetField) AddInteger(a int64) *UpdateExpression {
	v := strconv.FormatInt(a, 10)
	attr := &dynamodb.AttributeValue{
		NS: []*string{&v},
	}
	return Field.Add(attr)
}

func (Field *dynamoSetField) AddString(a string) *UpdateExpression {
	attr := &dynamodb.AttributeValue{
		SS: []*string{&a},
	}
	return Field.Add(attr)
}

func (Field *dynamoSetField) Delete(a *dynamodb.AttributeValue) *UpdateExpression {
	f := func(c uint) (string, map[string]*string, map[string]interface{}, uint) {
		ph := generatePlaceholder(a, c)
		s := fmt.Sprintf(Field.name+" %v", ph)
		m := map[string]interface{}{ph: []interface{}{a}}
		c++
		return s, nil, m, c
	}
	return &UpdateExpression{op: "DELETE", f: f}
}

func (Field *dynamoSetField) DeleteFloat(a float64) *UpdateExpression {
	v := strconv.FormatFloat(a, 'E', -1, 64)
	attr := &dynamodb.AttributeValue{
		NS: []*string{&v},
	}
	return Field.Delete(attr)
}
func (Field *dynamoSetField) DeleteInteger(a int64) *UpdateExpression {
	v := strconv.FormatInt(a, 10)
	attr := &dynamodb.AttributeValue{
		NS: []*string{&v},
	}
	return Field.Delete(attr)
}

func (Field *dynamoSetField) DeleteString(a string) *UpdateExpression {
	attr := &dynamodb.AttributeValue{
		SS: []*string{&a},
	}
	return Field.Delete(attr)
}

/*Increment a numeric counter Field*/
func (Field *Numeric) Increment(by uint) *UpdateExpression {
	return Field.Add(float64(by))
}

/*Decrement a numeric counter Field*/
func (Field *Numeric) Decrement(by uint) *UpdateExpression {
	return Field.Add(-float64(by))
}
