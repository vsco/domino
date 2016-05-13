package domino

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type DynamoDBIFace interface {
	CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	DeleteTable(input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error)
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
	Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
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

type Empty struct {
	dynamoField
}

type Numeric struct {
	dynamoValueField
}
type NumericSet struct {
	dynamoCollectionField
}
type String struct {
	dynamoValueField
}
type StringSet struct {
	dynamoCollectionField
}
type Binary struct {
	dynamoValueField
}
type BinarySet struct {
	dynamoCollectionField
}
type Bool struct {
	dynamoValueField
}

type List struct {
	dynamoCollectionField
}

type Map struct {
	dynamoCollectionField
}

func EmptyField() Empty {
	return Empty{
		dynamoField{
			empty: true,
			_type: NULL,
		},
	}
}

func NumericField(name string) Numeric {
	return Numeric{
		dynamoValueField{
			dynamoField{
				name:  name,
				_type: N,
			},
		},
	}
}

func NumericSetField(name string) NumericSet {
	return NumericSet{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: NS,
			},
		},
	}
}

func StringField(name string) String {
	return String{
		dynamoValueField{
			dynamoField{
				name:  name,
				_type: S,
			},
		},
	}
}

func BinaryField(name string) Binary {
	return Binary{
		dynamoValueField{
			dynamoField{
				name:  name,
				_type: B,
			},
		},
	}
}
func BinarySetField(name string) BinarySet {
	return BinarySet{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: BS,
			},
		},
	}
}

func StringSetField(name string) StringSet {
	return StringSet{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: SS,
			},
		},
	}
}

func ListField(name string) List {
	return List{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: L,
			},
		},
	}
}

func MapField(name string) Map {
	return Map{
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
	PartitionKey interface{}
	RangeKey     interface{}
}

/***************************************************************************************/
/************************************** GetItem ****************************************/
/***************************************************************************************/
type get dynamodb.GetItemInput

/*Primary constructor for creating a  get item query*/
func (table DynamoTable) GetItem(key KeyValue) *get {
	q := get(dynamodb.GetItemInput{})
	q.TableName = &table.Name
	appendAttribute(&q.Key, table.PartitionKey.Name(), key.PartitionKey)
	if !table.RangeKey.IsEmpty() {
		appendAttribute(&q.Key, table.RangeKey.Name(), key.RangeKey)
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
func (d *get) ExecuteWith(dynamo DynamoDBIFace, item interface{}) (r interface{}, err error) {
	out, err := dynamo.GetItem(d.Build())
	if err != nil {
		err = handleAwsErr(err)
		return
	}
	if out.Item != nil && len(out.Item) > 0 {
		r = item
		err = dynamodbattribute.UnmarshalMap(out.Item, r)
		if err != nil {
			err = handleAwsErr(err)
			return
		}
	}

	return
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
			table.PartitionKey.Name(): kv.PartitionKey,
		}
		if !table.RangeKey.IsEmpty() {
			m[table.RangeKey.Name()] = kv.RangeKey
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

	retry := 0
Execute:

	out, err := dynamo.BatchGetItem(d.Build())
	if err != nil {
		return handleAwsErr(err)
	}
	for _, r := range out.Responses {
		for _, item := range r {
			err = dynamodbattribute.UnmarshalMap(item, nextItem())
			if err != nil {
				return handleAwsErr(err)
			}
		}
	}
	if out.UnprocessedKeys != nil && len(out.UnprocessedKeys) > 0 {
		d.RequestItems = out.UnprocessedKeys
		retry++
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
	q.Item, _ = dynamodbattribute.MarshalMap(i)
	return &q
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

func (d *put) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.PutItem(d.Build())
	if err != nil {
		return handleAwsErr(err)
	}
	return nil
}

/***************************************************************************************/
/************************************** BatchPutItem *********************************/
/***************************************************************************************/
type batchPut struct {
	batches []dynamodb.BatchWriteItemInput
	table   DynamoTable
}

func (table DynamoTable) BatchWriteItem() *batchPut {
	r := batchPut{
		[]dynamodb.BatchWriteItemInput{},
		table,
	}
	return &r
}

func (d *batchPut) writeItems(putOnly bool, items ...interface{}) *batchPut {
	batches := []dynamodb.BatchWriteItemInput{}
	batchCount := len(items)/25 + 1

	for i := 1; i <= batchCount; i++ {
		batch := dynamodb.BatchWriteItemInput{
			RequestItems: make(map[string][]*dynamodb.WriteRequest),
		}
		puts := []*dynamodb.WriteRequest{}

		for len(items) > 0 && len(puts) < 25 {
			item := items[0]
			items = items[1:]
			dynamoItem, err := dynamodbattribute.MarshalMap(item)
			if err != nil {
				panic(err)
			}
			var write *dynamodb.WriteRequest
			if putOnly {
				write = &dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: dynamoItem,
					},
				}
			} else {
				write = &dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: dynamoItem,
					},
				}
			}

			puts = append(puts, write)
		}

		batch.RequestItems[d.table.Name] = puts
		batches = append(batches, batch)
	}
	d.batches = append(d.batches, batches...)
	return d
}

func (d *batchPut) PutItems(items ...interface{}) *batchPut {
	d.writeItems(true, items...)
	return d
}
func (d *batchPut) DeleteItems(keys ...KeyValue) *batchPut {
	a := []interface{}{}
	for _, key := range keys {
		m := map[string]interface{}{}
		appendKeyInterface(&m, d.table, key)
		a = append(a, m)
	}
	d.writeItems(false, a...)
	return d
}

func (d *batchPut) Build() []dynamodb.BatchWriteItemInput {
	return d.batches
}

func (d *batchPut) ExecuteWith(dynamo DynamoDBIFace, unprocessedItem func() interface{}) error {

	for _, batch := range d.Build() {
		out, err := dynamo.BatchWriteItem(&batch)
		if err != nil {
			return handleAwsErr(err)
		}

		for _, items := range out.UnprocessedItems {
			for _, item := range items {
				err = dynamodbattribute.UnmarshalMap(item.PutRequest.Item, unprocessedItem())
				if err != nil {
					return handleAwsErr(err)
				}
			}
		}
	}

	return nil
}

/***************************************************************************************/
/*************************************** DeleteItem ************************************/
/***************************************************************************************/
type deleteItem dynamodb.DeleteItemInput

func (table DynamoTable) DeleteItem(key KeyValue) *deleteItem {
	q := deleteItem(dynamodb.DeleteItemInput{})
	q.TableName = &table.Name
	appendKeyAttribute(&q.Key, table, key)
	return &q
}

func (d *deleteItem) SetConditionExpression(c Expression) *deleteItem {
	s, m, _ := c.construct(1)
	d.ConditionExpression = &s
	d.ExpressionAttributeValues, _ = dynamodbattribute.MarshalMap(m)
	return d
}

func (d *deleteItem) Build() *dynamodb.DeleteItemInput {
	r := dynamodb.DeleteItemInput(*d)
	return &r
}

func (d *deleteItem) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.DeleteItem(d.Build())
	if err != nil {
		return handleAwsErr(err)
	}
	return nil
}

/***************************************************************************************/
/*********************************** UpdateItem ****************************************/
/***************************************************************************************/
type update dynamodb.UpdateItemInput

func (table DynamoTable) UpdateItem(key KeyValue) *update {

	q := update(dynamodb.UpdateItemInput{})
	q.TableName = &table.Name

	appendKeyAttribute(&q.Key, table, key)

	return &q
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

	c := uint(100)
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

func (d *update) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.UpdateItem(d.Build())
	if err != nil {
		return handleAwsErr(err)
	}
	return nil
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
		appendAttribute(&q.ExpressionAttributeValues, k, v)
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

func (d *query) SetLimit(limit int) *query {
	s := int64(limit)
	d.Limit = &s
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
		appendAttribute(&d.ExpressionAttributeValues, k, v)
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

func (d *query) ExecuteWith(dynamodb DynamoDBIFace, nextItem interface{}) (c chan interface{}, e chan error) {

	c = make(chan interface{})
	e = make(chan error)

	go func() {
		defer close(c)
		defer close(e)

		var count int64 = 0
	Execute:
		if d.Limit != nil && count >= *d.Limit {
			return
		}
		out, err := dynamodb.Query(d.Build())
		if err != nil {
			e <- handleAwsErr(err)
			return
		}

		for _, item := range out.Items {
			err = dynamodbattribute.UnmarshalMap(item, &nextItem)

			if err != nil {
				e <- handleAwsErr(err)
				return
			} else {
				count++
				c <- nextItem
			}
		}

		if out.LastEvaluatedKey != nil {
			d.ExclusiveStartKey = out.LastEvaluatedKey
			goto Execute
		}
		return
	}()

	return
}

/***************************************************************************************/
/********************************************** Scan **********************************/
/***************************************************************************************/
type scan dynamodb.ScanInput

func (table DynamoTable) Scan() *scan {
	q := scan(dynamodb.ScanInput{})
	q.TableName = &table.Name
	return &q
}

func (d *scan) SetConsistentRead(c bool) *scan {
	(*d).ConsistentRead = &c
	return d
}
func (d *scan) SetAttributesToGet(fields []dynamoField) *scan {
	a := make([]*string, len(fields))
	for i, f := range fields {
		v := f.Name()
		a[i] = &v
	}
	(*d).AttributesToGet = a
	return d
}

func (d *scan) SetLimit(limit int) *scan {
	s := int64(limit)
	d.Limit = &s
	return d
}

func (d *scan) SetFilterExpression(c Expression) *scan {
	s, m, _ := c.construct(1)
	d.FilterExpression = &s

	for k, v := range m {
		appendAttribute(&d.ExpressionAttributeValues, k, v)
	}
	return d
}

func (d *scan) SetLocalIndex(idx LocalSecondaryIndex) *scan {
	d.IndexName = &idx.Name
	return d
}

func (d *scan) SetGlobalIndex(idx GlobalSecondaryIndex) *scan {
	d.IndexName = &idx.Name
	return d
}

func (d *scan) Build() *dynamodb.ScanInput {
	r := dynamodb.ScanInput(*d)
	return &r
}

func (d *scan) ExecuteWith(dynamodb DynamoDBIFace, nextItem interface{}) (c chan interface{}, e chan error) {

	c = make(chan interface{})
	e = make(chan error)

	go func() {
		defer close(c)
		defer close(e)

		var count int64 = 0
	Execute:
		if d.Limit != nil && count >= *d.Limit {
			return
		}
		out, err := dynamodb.Scan(d.Build())
		if err != nil {
			e <- handleAwsErr(err)
			return
		}

		for _, item := range out.Items {
			err = dynamodbattribute.UnmarshalMap(item, &nextItem)

			if err != nil {
				e <- handleAwsErr(err)
				return
			} else {
				count++
				c <- nextItem
			}
		}

		if out.LastEvaluatedKey != nil {
			d.ExclusiveStartKey = out.LastEvaluatedKey
			goto Execute
		}
		return
	}()

	return
}

/**********************************************************************************************/
/********************************************** Create Table **********************************/
/**********************************************************************************************/
type createTable dynamodb.CreateTableInput

func (table DynamoTable) CreateTable() *createTable {
	pk := table.PartitionKey.Name()
	pkt := "HASH"
	pktt := table.PartitionKey.Type()

	k := []*dynamodb.KeySchemaElement{
		&dynamodb.KeySchemaElement{
			AttributeName: &pk,
			KeyType:       &pkt,
		},
	}
	r := int64(100)
	w := int64(100)
	p := &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  &r,
		WriteCapacityUnits: &w,
	}

	a := []*dynamodb.AttributeDefinition{
		&dynamodb.AttributeDefinition{
			AttributeName: &pk,
			AttributeType: &pktt,
		},
	}

	if !table.RangeKey.IsEmpty() {
		rk := table.RangeKey.Name()
		rkt := "RANGE"
		rktt := table.RangeKey.Type()
		k = append(k, &dynamodb.KeySchemaElement{AttributeName: &rk, KeyType: &rkt})
		a = append(a, &dynamodb.AttributeDefinition{AttributeName: &rk, AttributeType: &rktt})
	}
	t := dynamodb.CreateTableInput{
		TableName:             &table.Name,
		KeySchema:             k,
		ProvisionedThroughput: p,
		AttributeDefinitions:  a,
	}
	c := createTable(t)
	return &c
}

func (d *createTable) Build() *dynamodb.CreateTableInput {
	r := dynamodb.CreateTableInput(*d)
	return &r
}

func (d *createTable) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.CreateTable(d.Build())
	return handleAwsErr(err)
}

/**********************************************************************************************/
/********************************************** Delete Table **********************************/
/**********************************************************************************************/
type deleteTable dynamodb.DeleteTableInput

func (table DynamoTable) DeleteTable() *deleteTable {
	r := deleteTable(dynamodb.DeleteTableInput{TableName: &table.Name})
	return &r
}

func (d *deleteTable) Build() *dynamodb.DeleteTableInput {
	r := dynamodb.DeleteTableInput(*d)
	return &r
}

func (d *deleteTable) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.DeleteTable(d.Build())
	return handleAwsErr(err)
}

/*****************************************   Helpers  ******************************************/
func appendKeyInterface(m *map[string]interface{}, table DynamoTable, key KeyValue) {
	if *m == nil {
		*m = map[string]interface{}{}
	}
	(*m)[table.PartitionKey.Name()] = key.PartitionKey

	if !table.RangeKey.IsEmpty() {
		(*m)[table.RangeKey.Name()] = key.RangeKey
	}

}
func appendKeyAttribute(m *map[string]*dynamodb.AttributeValue, table DynamoTable, key KeyValue) (err error) {
	err = appendAttribute(m, table.PartitionKey.Name(), key.PartitionKey)
	if err != nil {
		return
	} else if !table.RangeKey.IsEmpty() {
		err = appendAttribute(m, table.RangeKey.Name(), key.RangeKey)
		if err != nil {
			return
		}
	}
	return
}

func appendAttribute(m *map[string]*dynamodb.AttributeValue, key string, value interface{}) (err error) {
	if *m == nil {
		*m = make(map[string]*dynamodb.AttributeValue)
	}
	v, err := dynamodbattribute.Marshal(value)
	if err == nil {
		(*m)[key] = v
	}
	return
}

func handleAwsErr(err error) error {
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Get error details
			fmt.Println("Error:", awsErr.Code(), awsErr.Message())

			// Prints out full error message, including original error if there was one.
			fmt.Println("Error:", awsErr.Error())

			// Get original error
			if origErr := awsErr.OrigErr(); origErr != nil {
				// operate on original error.
			}
		} else {
			fmt.Println(err.Error())
		}
	}
	return err
}
