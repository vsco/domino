package godynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type DynamoDBIFace interface {
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
}

/*A static table definition representing a dynamo table*/
type DynamoTable struct {
	Name             string
	PartitionKeyName string
	RangeKeyName     *string //Optional param. If no range key set to nil
}

/*Key values for use in creating queries*/
type KeyValue struct {
	partitionKey interface{}
	rangeKey     interface{}
}

type Path string
type Type string

/*Attribute type enumeration*/
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

type comparator struct(
	neq = "<>"
	lt = "<"
	lte = "<="
	gt = ">"
	gte = ">="
)

type Condition struct {
	/*All adjacent Conditions ar OR'd together*/
	next          *Condition
	expression string
}

func (c *Condition) Or () *Condition {
	n := Condition
}
func (c *Condition) AttributeExists(a Path) *Condition {
	c.attrExists = append(c.attrExists, a)
	return c
}

func (c *Condition) AttributeNotExists(a Path) *Condition {
	c.attrExists = append(c.attrExists, a)
	return c
}

func (c * Condition) BeginsWith(p Path, a string) *Condition {

}

/*GetItemInput*/
type GetItem dynamodb.GetItemInput

/*Primary constructor for creating a  get item query*/
func (table DynamoTable) GetItem(key KeyValue) *GetItem {
	q := GetItem(dynamodb.GetItemInput{})
	t := (&q).SetTable(table.Name).SetKey(table.PartitionKeyName, key.partitionKey)
	if table.RangeKeyName != nil {
		t.SetKey(*table.RangeKeyName, key.rangeKey)
	}
	return t
}

func (d *GetItem) SetTable(name string) *GetItem {
	d.TableName = &name
	return d
}
func (d *GetItem) SetKey(name string, value interface{}) *GetItem {
	appendMap(&(*d).Key, name, value)
	return d
}
func (d *GetItem) GetAttributes(attribs ...string) *GetItem {
	a := (*d).AttributesToGet
	(*d).AttributesToGet = append(a, aws.StringSlice(attribs)...)
	return d
}
func (d *GetItem) SetConsistentRead(c bool) *GetItem {
	(*d).ConsistentRead = &c
	return d
}

/*Must call this method to create a GetItemInput object for use in aws dynamodb api*/
func (d *GetItem) Build() *dynamodb.GetItemInput {
	r := dynamodb.GetItemInput(*d)
	return &r
}

/*PutItemInput*/
type PutItem dynamodb.PutItemInput

func (table DynamoTable) PutItem(item *interface{}) *PutItem {
	q := PutItem(dynamodb.PutItemInput{})
	t := (&q).SetTable(table.Name)
	return t
}

func (d *PutItem) SetTable(name string) *PutItem {
	d.TableName = &name
	return d
}

func (d *PutItem) ReturnOld() *PutItem {
	s := "ALL_OLD"
	d.ReturnValues = &s
	return d
}

func (d *PutItem) Conditions(func(*Condition) Condition) *PutItem {
	return d
}

/*Helpers*/
func appendMap(m *map[string]*dynamodb.AttributeValue, key string, value interface{}) (*map[string]*dynamodb.AttributeValue, error) {
	if *m == nil {
		*m = make(map[string]*dynamodb.AttributeValue)
	}
	v, err := dynamodbattribute.Marshal(value)
	if err == nil {
		(*m)[key] = v
	}
	return m, err
}
