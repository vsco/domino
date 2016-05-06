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

type DynamoTable struct {
	Name             string
	PartitionKeyName string
	RangeKeyName     *string //Optional param. If no range key set to nil
}

type KeyValue struct {
	partitionKey interface{}
	rangeKey     interface{}
}

/*GetItemInput*/
type GetItem dynamodb.GetItemInput

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
func (d *GetItem) Build() *dynamodb.GetItemInput {
	r := dynamodb.GetItemInput(*d)
	return &r
}

/*PutItemInput*/
type PutItem dynamodb.PutItemInput

func (table DynamoTable) PutItem(item *interface{}) *PutItem {
	q := PutItem(dynamodb.PutItemInput{})
	t := (&q).SetTable(table.Name)

	return (&q)
}

func (d *PutItem) SetTable(name string) *PutItem {
	d.TableName = &name
	return d
}

func (d *PutItem) ReturnOld() *PutItem {
	d.ReturnValues = &"ALL_OLD"
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
