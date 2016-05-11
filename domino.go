package domino

import (
	// "fmt"
	// "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type DynamoDBIFace interface {
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
	UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
}

const (
	S    = "S"
	SS   = "SS"
	N    = "N"
	NS   = "NS"
	B    = "B"
	BS   = "BS"
	BOOL = "Bool"
	NULL = "Null"
	L    = "List"
	M    = "M"
)

/*A static table definition representing a dynamo table*/
type DynamoTable struct {
	Name         string
	PartitionKey DynamoFieldIFace
	RangeKey     DynamoFieldIFace //Optional param. If no range key set to EmptyDynamoField()
}

type DynamoFieldIFace interface {
	Name() string
	Type() string
	IsEmpty() bool
}

type dynamoField struct {
	name  string
	_type string
	empty bool //If true, this represents an empty field
}

type dynamoValueField struct {
	dynamoField
}

type dynamoCollectionField struct {
	dynamoField
}

func (d dynamoField) Name() string {
	return d.name
}
func (d dynamoField) Type() string {
	return d._type
}
func (d dynamoField) IsEmpty() bool {
	return d.empty
}

type emptyDynamoField struct {
	dynamoField
}

type dynamoFieldNumeric struct {
	dynamoValueField
}
type dynamoFieldNumericSet struct {
	dynamoCollectionField
}
type dynamoFieldString struct {
	dynamoValueField
}
type dynamoFieldStringSet struct {
	dynamoCollectionField
}
type dynamoFieldBinary struct {
	dynamoValueField
}
type dynamoFieldBinarySet struct {
	dynamoCollectionField
}
type dynamoFieldBool struct {
	dynamoValueField
}

type dynamoFieldList struct {
	dynamoCollectionField
}

type dynamoFieldMap struct {
	dynamoCollectionField
}

func EmptyDynamoField() emptyDynamoField {
	return emptyDynamoField{
		dynamoField{
			empty: true,
			_type: NULL,
		},
	}
}

func DynamoFieldNumeric(name string) dynamoFieldNumeric {
	return dynamoFieldNumeric{
		dynamoValueField{
			dynamoField{
				name:  name,
				_type: N,
			},
		},
	}
}

func DynamoFieldNumericSet(name string) dynamoFieldNumericSet {
	return dynamoFieldNumericSet{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: NS,
			},
		},
	}
}

func DynamoFieldString(name string) dynamoFieldString {
	return dynamoFieldString{
		dynamoValueField{
			dynamoField{
				name:  name,
				_type: S,
			},
		},
	}
}

func DynamoFieldBinary(name string) dynamoFieldBinary {
	return dynamoFieldBinary{
		dynamoValueField{
			dynamoField{
				name:  name,
				_type: B,
			},
		},
	}
}
func DynamoFieldBinarySet(name string) dynamoFieldBinarySet {
	return dynamoFieldBinarySet{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: BS,
			},
		},
	}
}

func DynamoFieldStringSet(name string) dynamoFieldStringSet {
	return dynamoFieldStringSet{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: SS,
			},
		},
	}
}

func DynamoFieldList(name string) dynamoFieldList {
	return dynamoFieldList{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: L,
			},
		},
	}
}

func DynamoFieldMap(name string) dynamoFieldMap {
	return dynamoFieldMap{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: L,
			},
		},
	}
}

type LocalSecondaryIndex struct {
	Name    string
	SortKey DynamoFieldIFace
}

type GlobalSecondaryIndex struct {
	Name         string
	PartitionKey DynamoFieldIFace
	RangeKey     DynamoFieldIFace //Optional param. If no range key set to nil
}

/*Key values for use in creating queries*/
type KeyValue struct {
	partitionKey interface{}
	rangeKey     interface{}
}

/***************************************************************************************/
/************************************** GetItem ****************************************/
/***************************************************************************************/
type get dynamodb.GetItemInput

/*Primary constructor for creating a  get item query*/
func (table DynamoTable) GetItem(key KeyValue) *get {
	q := get(dynamodb.GetItemInput{})
	q.TableName = &table.Name
	appendMap(&q.Key, table.PartitionKey.Name(), key.partitionKey)
	if !table.RangeKey.IsEmpty() {
		appendMap(&q.Key, table.RangeKey.Name(), key.rangeKey)
	}
	return &q
}

func (d *get) SetConsistentRead(c bool) *get {
	(*d).ConsistentRead = &c
	return d
}

/*Must call this method to create a GetItemInput object for use in aws dynamodb api*/
func (d *get) Build() *dynamodb.GetItemInput {
	r := dynamodb.GetItemInput(*d)
	return &r
}

/*Execute a dynamo getitem call, hydrating the passed in struct on return or returning error*/
func (d *get) ExecuteWith(dynamo DynamoDBIFace, item interface{}) error {
	out, err := dynamo.GetItem(d.Build())
	if err != nil {
		return err
	}
	err = dynamodbattribute.UnmarshalMap(out.Item, item)
	if err != nil {
		return err
	}
	return nil
}

/***************************************************************************************/
/************************************** BatchGetItem ***********************************/
/***************************************************************************************/
type batchGet dynamodb.BatchGetItemInput

func (table DynamoTable) BatchGetItem(items ...KeyValue) *batchGet {
	k := make(map[string]*dynamodb.KeysAndAttributes)
	keysAndAttribs := dynamodb.KeysAndAttributes{}
	k[table.Name] = &keysAndAttribs

	for _, kv := range items {
		m := map[string]interface{}{
			table.PartitionKey.Name(): kv.partitionKey,
		}
		if !table.RangeKey.IsEmpty() {
			m[table.RangeKey.Name()] = kv.rangeKey
		}

		attributes, err := dynamodbattribute.MarshalMap(m)

		if err != nil {
			panic(err)
		}
		keysAndAttribs.Keys = append(keysAndAttribs.Keys, attributes)
	}

	q := batchGet(dynamodb.BatchGetItemInput{})
	q.RequestItems = k
	return &q
}

func (d *batchGet) SetConsistentRead(c bool) *batchGet {
	for _, ka := range d.RequestItems {
		(*ka).ConsistentRead = &c
	}
	return d
}

func (d *batchGet) Build() *dynamodb.BatchGetItemInput {
	r := dynamodb.BatchGetItemInput(*d)
	return &r
}

func (d *batchGet) ExecuteWith(dynamo DynamoDBIFace, nextItem func() interface{}) error {

Execute:

	out, err := dynamo.BatchGetItem(d.Build())
	if err != nil {
		return err
	}
	for _, r := range out.Responses {
		for _, item := range r {
			err = dynamodbattribute.UnmarshalMap(item, nextItem())
			if err != nil {
				return err
			}
		}
	}
	if out.UnprocessedKeys != nil && len(out.UnprocessedKeys) > 0 {
		d.RequestItems = out.UnprocessedKeys
		goto Execute
	}

	return nil
}

/***************************************************************************************/
/************************************** PutItem ****************************************/
/***************************************************************************************/
type put dynamodb.PutItemInput

func (table DynamoTable) PutItem(i interface{}) *put {

	q := put(dynamodb.PutItemInput{})
	q.TableName = &table.Name
	q.Item, _ = dynamodbattribute.ConvertToMap(i)
	return &q
}

func (d *put) ReturnOld() *put {
	s := "ALL_OLD"
	d.ReturnValues = &s
	return d
}

func (d *put) SetConditionExpression(c Expression) *put {
	s, m, _ := c.construct(1)

	d.ConditionExpression = &s
	d.ExpressionAttributeValues, _ = dynamodbattribute.MarshalMap(m)

	return d
}

func (d *put) Build() *dynamodb.PutItemInput {
	r := dynamodb.PutItemInput(*d)
	return &r
}

/***************************************************************************************/
/************************************** BatchPutItem *********************************/
/***************************************************************************************/
type batchPut dynamodb.BatchWriteItemInput

func (table DynamoTable) BatchPutItem(items ...interface{}) *batchPut {
	puts := make([]*dynamodb.WriteRequest, len(items))

	for i, put := range items {
		item, err := dynamodbattribute.MarshalMap(put)
		if err != nil {
			panic(err)
		}
		puts[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		}
	}
	q := batchPut(dynamodb.BatchWriteItemInput{
		RequestItems: make(map[string][]*dynamodb.WriteRequest),
	})
	q.RequestItems[table.Name] = puts
	return &q
}

func (d *batchPut) Build() *dynamodb.BatchWriteItemInput {
	r := dynamodb.BatchWriteItemInput(*d)
	return &r
}

/***************************************************************************************/
/*************************************** DeleteItem ************************************/
/***************************************************************************************/
type deleteItem dynamodb.DeleteItemInput

func (table DynamoTable) DeleteItem(key KeyValue) *deleteItem {
	q := deleteItem(dynamodb.DeleteItemInput{})
	q.TableName = &table.Name
	appendMap(&q.Key, table.PartitionKey.Name(), key.partitionKey)
	if !table.RangeKey.IsEmpty() {
		appendMap(&q.Key, table.RangeKey.Name(), key.rangeKey)
	}
	return &q
}

func (d *deleteItem) SetConditionExpression(c Expression) *deleteItem {
	s, m, _ := c.construct(1)

	d.ConditionExpression = &s
	d.ExpressionAttributeValues, _ = dynamodbattribute.MarshalMap(m)

	return d
}
func (d *deleteItem) ReturnOld() *deleteItem {
	s := "ALL_OLD"
	d.ReturnValues = &s
	return d
}
func (d *deleteItem) Build() *dynamodb.DeleteItemInput {
	r := dynamodb.DeleteItemInput(*d)
	return &r
}

/***************************************************************************************/
/************************************** BatchDeleteItem *********************************/
/***************************************************************************************/
type batchDelete dynamodb.BatchWriteItemInput

func (table DynamoTable) BatchDeleteItem(items ...KeyValue) *batchDelete {
	puts := make([]*dynamodb.WriteRequest, len(items))

	for i, kv := range items {
		m := map[string]interface{}{
			table.PartitionKey.Name(): kv.partitionKey,
		}
		if !table.RangeKey.IsEmpty() {
			m[table.RangeKey.Name()] = kv.rangeKey
		}

		key, err := dynamodbattribute.MarshalMap(m)
		if err != nil {
			panic(err)
		}
		puts[i] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: key,
			},
		}
	}
	q := batchDelete(dynamodb.BatchWriteItemInput{
		RequestItems: make(map[string][]*dynamodb.WriteRequest),
	})
	q.RequestItems[table.Name] = puts
	return &q
}

func (d *batchDelete) Build() *dynamodb.BatchWriteItemInput {
	r := dynamodb.BatchWriteItemInput(*d)
	return &r
}

/***************************************************************************************/
/*********************************** UpdateItem ****************************************/
/***************************************************************************************/
type update dynamodb.UpdateItemInput

func (table DynamoTable) UpdateItem(key KeyValue) *update {

	q := update(dynamodb.UpdateItemInput{})
	q.TableName = &table.Name
	appendMap(&q.Key, table.PartitionKey.Name(), key.partitionKey)
	if !table.RangeKey.IsEmpty() {
		appendMap(&q.Key, table.RangeKey.Name(), key.rangeKey)
	}
	return &q
}

func (d *update) ReturnOld() *update {
	s := "ALL_OLD"
	d.ReturnValues = &s
	return d
}

func (d *update) SetConditionExpression(c Expression) *update {
	s, m, _ := c.construct(1)
	d.ConditionExpression = &s
	ea, err := dynamodbattribute.MarshalMap(m)
	if err != nil {
		panic(err)
	}
	if d.ExpressionAttributeValues == nil {
		d.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	for k, v := range ea {
		d.ExpressionAttributeValues[k] = v
	}
	return d
}

func (d *update) SetUpdateExpression(exprs ...*UpdateExpression) *update {
	m := make(map[string]interface{})
	ms := make(map[string]string)

	c := 100
	for _, expr := range exprs {
		s, mr, nc := expr.f(c)
		c = nc
		for k, v := range mr {
			m[k] = v
		}
		if ms[expr.op] == "" {
			ms[expr.op] = s
		} else {
			ms[expr.op] += ", " + s
		}
	}

	var s string
	for k, v := range ms {
		s += k + " " + v + " "
	}

	d.UpdateExpression = &s
	ea, err := dynamodbattribute.MarshalMap(m)
	if err != nil {
		panic(err)
	}
	if d.ExpressionAttributeValues == nil {
		d.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	for k, v := range ea {
		d.ExpressionAttributeValues[k] = v
	}
	return d
}

func (d *update) Build() *dynamodb.UpdateItemInput {
	r := dynamodb.UpdateItemInput(*d)
	return &r
}

/***************************************************************************************/
/********************************************** Query **********************************/
/***************************************************************************************/
type query dynamodb.QueryInput

func (table DynamoTable) Query(partitionKeyCondition keyCondition, rangeKeyCondition *keyCondition) *query {
	q := query(dynamodb.QueryInput{})
	var e Expression
	if rangeKeyCondition != nil {
		e = And(partitionKeyCondition, *rangeKeyCondition)
	} else {
		e = partitionKeyCondition
	}

	s, m, _ := e.construct(0)
	q.TableName = &table.Name
	q.KeyConditionExpression = &s
	for k, v := range m {
		appendMap(&q.ExpressionAttributeValues, k, v)
	}

	return &q
}

func (d *query) SetConsistentRead(c bool) *query {
	(*d).ConsistentRead = &c
	return d
}
func (d *query) SetAttributesToGet(fields []dynamoField) *query {
	a := make([]*string, len(fields))
	for i, f := range fields {
		v := f.Name()
		a[i] = &v
	}
	(*d).AttributesToGet = a
	return d
}

/*func (d *query) SetExclusiveStartKey(fields []dynamoField) *query {

}*/

func (d *query) SetLimit(limit int64) *query {
	d.Limit = &limit
	return d
}

func (d *query) SetScanForward(forward bool) *query {
	d.ScanIndexForward = &forward
	return d
}

func (d *query) SetFilterExpression(c Expression) *query {
	s, m, _ := c.construct(1)
	d.FilterExpression = &s

	for k, v := range m {
		appendMap(&d.ExpressionAttributeValues, k, v)
	}
	return d
}

func (d *query) SetLocalIndex(idx LocalSecondaryIndex) *query {
	d.IndexName = &idx.Name
	return d
}

func (d *query) SetGlobalIndex(idx GlobalSecondaryIndex) *query {
	d.IndexName = &idx.Name
	return d
}

func (d *query) Build() *dynamodb.QueryInput {
	r := dynamodb.QueryInput(*d)
	return &r
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
