package domino

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

/*DynamoDBIFace is the interface to the underlying aws dyanmo db api*/
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
	dS    = "S"
	dSS   = "SS"
	dN    = "N"
	dNS   = "NS"
	dB    = "B"
	dBS   = "BS"
	dBOOL = "Bool"
	dNULL = "Null"
	dL    = "List"
	dM    = "M"
)

/*DynamoTable is a static table definition representing a dynamo table*/
type DynamoTable struct {
	Name         string
	PartitionKey dynamoFieldIFace
	RangeKey     dynamoFieldIFace //Optional param. If no range key set to EmptyDynamoField()
}

type dynamoFieldIFace interface {
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

/*Empty - An empty dynamo field*/
type Empty struct {
	dynamoField
}

/*Numeric - A numeric dynamo field*/
type Numeric struct {
	dynamoValueField
}

/*NumericSet - A numeric set dynamo field*/
type NumericSet struct {
	dynamoCollectionField
}

/*String - A string dynamo field*/
type String struct {
	dynamoValueField
}

/*StringSet - A string set dynamo field*/
type StringSet struct {
	dynamoCollectionField
}

/*Binary - A binary dynamo field*/
type Binary struct {
	dynamoValueField
}

/*BinarySet - A binary dynamo field*/
type BinarySet struct {
	dynamoCollectionField
}

/*Bool - A boolean dynamo field*/
type Bool struct {
	dynamoValueField
}

/*List - A list dynamo field*/
type List struct {
	dynamoCollectionField
}

/*Map - A map dynamo field*/
type Map struct {
	dynamoCollectionField
}

/*EmptyField ... A constructor for an empty dynamo field*/
func EmptyField() Empty {
	return Empty{
		dynamoField{
			empty: true,
			_type: dNULL,
		},
	}
}

/*NumericField ... A constructor for a numeric dynamo field*/
func NumericField(name string) Numeric {
	return Numeric{
		dynamoValueField{
			dynamoField{
				name:  name,
				_type: dN,
			},
		},
	}
}

/*NumericSetField ... A constructor for a numeric set dynamo field*/
func NumericSetField(name string) NumericSet {
	return NumericSet{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: dNS,
			},
		},
	}
}

/*StringField ... A constructor for a string dynamo field*/
func StringField(name string) String {
	return String{
		dynamoValueField{
			dynamoField{
				name:  name,
				_type: dS,
			},
		},
	}
}

/*BinaryField ... A constructor for a binary dynamo field*/
func BinaryField(name string) Binary {
	return Binary{
		dynamoValueField{
			dynamoField{
				name:  name,
				_type: dB,
			},
		},
	}
}

/*BinarySetField ... A constructor for a binary set dynamo field*/
func BinarySetField(name string) BinarySet {
	return BinarySet{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: dBS,
			},
		},
	}
}

/*StringSetField ... A constructor for a string set dynamo field*/
func StringSetField(name string) StringSet {
	return StringSet{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: dSS,
			},
		},
	}
}

/*ListField ... A constructor for a list dynamo field*/
func ListField(name string) List {
	return List{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: dL,
			},
		},
	}
}

/*MapField ... A constructor for a map dynamo field*/
func MapField(name string) Map {
	return Map{
		dynamoCollectionField{
			dynamoField{
				name:  name,
				_type: dL,
			},
		},
	}
}

/*LocalSecondaryIndex ... Represents a dynamo local secondary index*/
type LocalSecondaryIndex struct {
	Name    string
	SortKey dynamoFieldIFace
}

/*GlobalSecondaryIndex ... Represents a dynamo global secondary index*/
type GlobalSecondaryIndex struct {
	Name         string
	PartitionKey dynamoFieldIFace
	RangeKey     dynamoFieldIFace //Optional param. If no range key set to nil
}

/*KeyValue ... A Key Value struct for use in GetItem and BatchWriteItem queries*/
type KeyValue struct {
	PartitionKey interface{}
	RangeKey     interface{}
}

/***************************************************************************************/
/************************************** GetItem ****************************************/
/***************************************************************************************/
type get dynamodb.GetItemInput

/*GetItem Primary constructor for creating a  get item query*/
func (table DynamoTable) GetItem(key KeyValue) *get {
	q := get(dynamodb.GetItemInput{})
	q.TableName = &table.Name
	appendAttribute(&q.Key, table.PartitionKey.Name(), key.PartitionKey)
	if !table.RangeKey.IsEmpty() {
		appendAttribute(&q.Key, table.RangeKey.Name(), key.RangeKey)
	}
	return &q
}

/*SetConsistentRead ... */
func (d *get) SetConsistentRead(c bool) *get {
	(*d).ConsistentRead = &c
	return d
}

func (d *get) build() *dynamodb.GetItemInput {
	r := dynamodb.GetItemInput(*d)
	return &r
}

/**
 ** ExecuteWith ... Execute a dynamo getitem call with a passed in dynamodb instance
 ** dynamo - The underlying dynamodb api
 ** item - The item pointer to be hyderated. I.e. if the table holds User object, item should be a pointer to a uninitialized
 **        User{} struct
 **
 ** Returns a tuple of the hydrated item struct, or an error
 */
func (d *get) ExecuteWith(dynamo DynamoDBIFace, item interface{}) (r interface{}, err error) {
	out, err := dynamo.GetItem(d.build())
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
type batchGet struct {
	input *dynamodb.BatchGetItemInput
	/*A set of mutational operations that might error out, i.e. not pure, and therefore not conducive to a fluent dsl*/
	delayedFunctions []func() error
}

/*BatchGetItem represents dynamo batch get item call*/
func (table DynamoTable) BatchGetItem(items ...KeyValue) *batchGet {
	/*Delay the attribute value construction, until build time*/
	input := &dynamodb.BatchGetItemInput{}
	delayed := func() error {
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
				return err
			}
			keysAndAttribs.Keys = append(keysAndAttribs.Keys, attributes)
		}
		(*input).RequestItems = k
		return nil
	}

	q := batchGet{
		input:            input,
		delayedFunctions: []func() error{delayed},
	}

	return &q
}

func (d *batchGet) SetConsistentRead(c bool) *batchGet {
	for _, ka := range d.input.RequestItems {
		(*ka).ConsistentRead = &c
	}
	return d
}

func (d *batchGet) build() (input *dynamodb.BatchGetItemInput, err error) {
	for _, function := range d.delayedFunctions {
		err = function()
		if err != nil {
			return
		}
	}
	input = (*dynamodb.BatchGetItemInput)((*d).input)

	return
}

/**
 ** ExecuteWith ... Execute a dynamo BatchGetItem call with a passed in dynamodb instance and next item pointer
 ** dynamo - The underlying dynamodb api
 ** nextItem - The item pointer function, which is called on each new object returned from dynamodb. The function should
 ** 		   store each item in an array before returning.
 **
 */
func (d *batchGet) ExecuteWith(dynamo DynamoDBIFace, nextItem func() interface{}) error {

	retry := 0
	input, err := d.build()
Execute:

	if err != nil {
		return err
	}
	out, err := dynamo.BatchGetItem(input)

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
		input.RequestItems = out.UnprocessedKeys
		retry++
		goto Execute
	}

	return nil
}

/***************************************************************************************/
/************************************** PutItem ****************************************/
/***************************************************************************************/
type put dynamodb.PutItemInput

/*PutItem represents dynamo put item call*/
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

func (d *put) build() *dynamodb.PutItemInput {
	r := dynamodb.PutItemInput(*d)
	return &r
}

/**
 ** ExecuteWith ... Execute a dynamo PutItem call with a passed in dynamodb instance
 ** dynamo - The underlying dynamodb api
 **
 */
func (d *put) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.PutItem(d.build())
	if err != nil {
		return handleAwsErr(err)
	}
	return nil
}

/***************************************************************************************/
/************************************** BatchPutItem *********************************/
/***************************************************************************************/
type batchPut struct {
	batches          []dynamodb.BatchWriteItemInput
	table            DynamoTable
	delayedFunctions []func() error
}

/*BatchWriteItem represents dynamo batch write item call*/
func (table DynamoTable) BatchWriteItem() *batchPut {
	r := batchPut{
		batches: []dynamodb.BatchWriteItemInput{},
		table:   table,
	}
	return &r
}

func (d *batchPut) writeItems(putOnly bool, items ...interface{}) *batchPut {
	delayed := func() error {
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
					return err
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
		return nil
	}
	d.delayedFunctions = append(d.delayedFunctions, delayed)

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

func (d *batchPut) build() (input []dynamodb.BatchWriteItemInput, err error) {
	for _, function := range d.delayedFunctions {
		if err = function(); err != nil {
			return
		}
	}
	input = d.batches
	return
}

/**
 ** ExecuteWith ... Execute a dynamo BatchWriteItem call with a passed in dynamodb instance and unprocessed item pointer function
 ** dynamo - The underlying dynamodb api
 ** unprocessedItem - The item pointer function, which is called on each object returned from dynamodb that could not be processed.
 ** 				The function should store each item pointer in an array before returning.
 **
 */
func (d *batchPut) ExecuteWith(dynamo DynamoDBIFace, unprocessedItem func() interface{}) error {

	batches, err := d.build()
	if err != nil {
		return err
	}
	for _, batch := range batches {
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

/*DeleteItem represents dynamo delete item call*/
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

func (d *deleteItem) build() *dynamodb.DeleteItemInput {
	r := dynamodb.DeleteItemInput(*d)
	return &r
}

/**
 ** ExecuteWith ... Execute a dynamo DeleteItem call with a passed in dynamodb instance
 ** dynamo - The underlying dynamodb api
 **
 */
func (d *deleteItem) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.DeleteItem(d.build())
	if err != nil {
		return handleAwsErr(err)
	}
	return nil
}

/***************************************************************************************/
/*********************************** UpdateItem ****************************************/
/***************************************************************************************/
type update struct {
	input            dynamodb.UpdateItemInput
	delayedFunctions []func() error
}

/*UpdateItem represents dynamo batch get item call*/
func (table DynamoTable) UpdateItem(key KeyValue) *update {
	q := update{input: dynamodb.UpdateItemInput{TableName: &table.Name}}
	appendKeyAttribute(&q.input.Key, table, key)
	return &q
}

func (d *update) SetConditionExpression(c Expression) *update {
	delayed := func() error {
		s, m, _ := c.construct(1)
		d.input.ConditionExpression = &s
		ea, err := dynamodbattribute.MarshalMap(m)
		if err != nil {
			return err
		}
		if d.input.ExpressionAttributeValues == nil {
			d.input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
		}
		for k, v := range ea {
			d.input.ExpressionAttributeValues[k] = v
		}
		return nil
	}
	d.delayedFunctions = append(d.delayedFunctions, delayed)
	return d
}

func (d *update) SetUpdateExpression(exprs ...*updateExpression) *update {
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

	d.input.UpdateExpression = &s
	ea, err := dynamodbattribute.MarshalMap(m)
	if err != nil {
		panic(err)
	}
	if d.input.ExpressionAttributeValues == nil {
		d.input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	for k, v := range ea {
		d.input.ExpressionAttributeValues[k] = v
	}
	return d
}

func (d *update) build() *dynamodb.UpdateItemInput {
	r := dynamodb.UpdateItemInput((*d).input)
	return &r
}

/**
 ** ExecuteWith ... Execute a dynamo BatchGetItem call with a passed in dynamodb instance
 ** dynamo - The underlying dynamodb api
 **
 */
func (d *update) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.UpdateItem(d.build())
	if err != nil {
		return handleAwsErr(err)
	}
	return nil
}

/***************************************************************************************/
/********************************************** Query **********************************/
/***************************************************************************************/
type query dynamodb.QueryInput

/*Query represents dynamo batch get item call*/
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

func (d *query) build() *dynamodb.QueryInput {
	r := dynamodb.QueryInput(*d)
	return &r
}

/**
 ** ExecuteWith ... Execute a dynamo BatchGetItem call with a passed in dynamodb instance and next item pointer
 ** dynamo - The underlying dynamodb api
 ** nextItem - The item pointer function, which is called on each new object returned from dynamodb. The function SHOULD NOT
 ** 		   store each item. It should simply return an empty struct pointer. Each of which is hydrated and pused on the
 ** 			returned channel.
 **
 */
func (d *query) ExecuteWith(dynamodb DynamoDBIFace, nextItem interface{}) (c chan interface{}, e chan error) {

	c = make(chan interface{})
	e = make(chan error)

	go func() {
		defer close(c)
		defer close(e)

		var count int64
	Execute:
		if d.Limit != nil && count >= *d.Limit {
			return
		}
		out, err := dynamodb.Query(d.build())
		if err != nil {
			e <- handleAwsErr(err)
			return
		}

		for _, item := range out.Items {
			err = dynamodbattribute.UnmarshalMap(item, &nextItem)

			if err != nil {
				e <- handleAwsErr(err)
				return
			}
			count++
			c <- nextItem
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

/*Scan represents dynamo scan item call*/
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

func (d *scan) build() *dynamodb.ScanInput {
	r := dynamodb.ScanInput(*d)
	return &r
}

/**
 ** ExecuteWith ... Execute a dynamo Scan call with a passed in dynamodb instance and next item pointer
 ** dynamo - The underlying dynamodb api
 ** nextItem - The item pointer function, which is called on each new object returned from dynamodb. The function SHOULD NOT
 ** 		   store each item. It should simply return an empty struct pointer. Each of which is hydrated and pushed on
 ** 		   the returned channel.
 **
 */
func (d *scan) ExecuteWith(dynamodb DynamoDBIFace, nextItem interface{}) (c chan interface{}, e chan error) {

	c = make(chan interface{})
	e = make(chan error)

	go func() {
		defer close(c)
		defer close(e)

		var count int64
	Execute:
		if d.Limit != nil && count >= *d.Limit {
			return
		}
		out, err := dynamodb.Scan(d.build())
		if err != nil {
			e <- handleAwsErr(err)
			return
		}

		for _, item := range out.Items {
			err = dynamodbattribute.UnmarshalMap(item, &nextItem)

			if err != nil {
				e <- handleAwsErr(err)
				return
			}
			count++
			c <- nextItem
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

func (d *createTable) build() *dynamodb.CreateTableInput {
	r := dynamodb.CreateTableInput(*d)
	return &r
}

func (d *createTable) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.CreateTable(d.build())
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

func (d *deleteTable) build() *dynamodb.DeleteTableInput {
	r := dynamodb.DeleteTableInput(*d)
	return &r
}

func (d *deleteTable) ExecuteWith(dynamo DynamoDBIFace) error {
	_, err := dynamo.DeleteTable(d.build())
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
			fmt.Errorf("Error: %v, %v", awsErr.Code(), awsErr.Message())
		} else {
			err.Error()
		}
	}
	return err
}
