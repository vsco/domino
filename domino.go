package domino

import (
	"context"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

/*DynamoDBIFace is the interface to the underlying aws dynamo db api*/
type DynamoDBIFace interface {
	CreateTableWithContext(aws.Context, *dynamodb.CreateTableInput, ...request.Option) (*dynamodb.CreateTableOutput, error)
	DeleteTableWithContext(aws.Context, *dynamodb.DeleteTableInput, ...request.Option) (*dynamodb.DeleteTableOutput, error)
	GetItemWithContext(aws.Context, *dynamodb.GetItemInput, ...request.Option) (*dynamodb.GetItemOutput, error)
	BatchGetItemWithContext(aws.Context, *dynamodb.BatchGetItemInput, ...request.Option) (*dynamodb.BatchGetItemOutput, error)
	PutItemWithContext(aws.Context, *dynamodb.PutItemInput, ...request.Option) (*dynamodb.PutItemOutput, error)
	QueryWithContext(aws.Context, *dynamodb.QueryInput, ...request.Option) (*dynamodb.QueryOutput, error)
	ScanWithContext(aws.Context, *dynamodb.ScanInput, ...request.Option) (*dynamodb.ScanOutput, error)
	UpdateItemWithContext(aws.Context, *dynamodb.UpdateItemInput, ...request.Option) (*dynamodb.UpdateItemOutput, error)
	DeleteItemWithContext(aws.Context, *dynamodb.DeleteItemInput, ...request.Option) (*dynamodb.DeleteItemOutput, error)
	BatchWriteItemWithContext(aws.Context, *dynamodb.BatchWriteItemInput, ...request.Option) (*dynamodb.BatchWriteItemOutput, error)
}

type DynamoDBValue map[string]*dynamodb.AttributeValue

// Loader is the interface that specifies the ability to deserialize and load data from dynamodb attrbiute value map
type Loader interface {
	LoadDynamoDBValue(av DynamoDBValue) (err error)
}

func deserializeTo(av DynamoDBValue, item interface{}) (err error) {
	if len(av) <= 0 {
		return
	}

	switch t := (item).(type) {
	case Loader:
		err = t.LoadDynamoDBValue(av)
	default:
		err = dynamodbattribute.UnmarshalMap(av, item)
	}
	return
}

// ToValue is the interface that specifies the ability to serialize data to a value that can be persisted in dynamodb
type ToValue interface {
	ToDynamoDBValue() (bm interface{})
}

func serialize(item interface{}) (av map[string]*dynamodb.AttributeValue, err error) {
	switch t := item.(type) {
	case ToValue:
		av, err = dynamodbattribute.MarshalMap(t.ToDynamoDBValue())
	default:
		av, err = dynamodbattribute.MarshalMap(item)
	}
	return
}

func marshal(m map[string]interface{}) (o map[string]*dynamodb.AttributeValue) {
	if len(m) <= 0 {
		return
	}
	o = map[string]*dynamodb.AttributeValue{}
	for k, v := range m {
		switch t := v.(type) {
		case *dynamodb.AttributeValue:
			o[k] = t
		default:
			var err error
			if o[k], err = dynamodbattribute.Marshal(t); err != nil {
				panic(err)
			}
		}
	}

	return
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

const (
	ProjectionTypeALL       = "ALL"
	ProjectionTypeINCLUDE   = "INCLUDE"
	ProjectionTypeKEYS_ONLY = "KEYS_ONLY"
)

/*DynamoTable is a static table definition representing a dynamo table*/
type DynamoTable struct {
	Name                   string
	PartitionKey           DynamoFieldIFace
	RangeKey               DynamoFieldIFace //Optional param. If no range key set to EmptyDynamoField()
	GlobalSecondaryIndexes []GlobalSecondaryIndex
	LocalSecondaryIndexes  []LocalSecondaryIndex
}

type DynamoFieldIFace interface {
	Name() string
	Type() string
	IsEmpty() bool
}

type DynamoField struct {
	name  string
	_type string
	empty bool //If true, this represents an empty field
}

type dynamoValueField struct {
	DynamoField
}

type dynamoCollectionField struct {
	DynamoField
}

type dynamoListField struct {
	dynamoCollectionField
}
type dynamoSetField struct {
	dynamoCollectionField
}

type dynamoMapField struct {
	DynamoField
}

func (d DynamoField) Name() string {
	return d.name
}
func (d DynamoField) Type() string {
	return d._type
}
func (d DynamoField) IsEmpty() bool {
	return d.empty
}

/*Empty - An empty dynamo field*/
type Empty struct {
	DynamoField
}

/*Numeric - A numeric dynamo field*/
type Numeric struct {
	dynamoValueField
}

/*NumericSet - A numeric set dynamo field*/
type NumericSet struct {
	dynamoSetField
}

/*String - A string dynamo field*/
type String struct {
	dynamoValueField
}

/*StringSet - A string set dynamo field*/
type StringSet struct {
	dynamoSetField
}

/*Binary - A binary dynamo field*/
type Binary struct {
	dynamoValueField
}

/*BinarySet - A binary dynamo field*/
type BinarySet struct {
	dynamoSetField
}

/*Bool - A boolean dynamo field*/
type Bool struct {
	dynamoValueField
}

/*List - A list dynamo field*/
type List struct {
	dynamoListField
}

/*Map - A map dynamo field*/
type Map struct {
	dynamoMapField
}

/*EmptyField ... A constructor for an empty dynamo field*/
func EmptyField() Empty {
	return Empty{
		DynamoField{
			empty: true,
			_type: dNULL,
		},
	}
}

/*NumericField ... A constructor for a numeric dynamo field*/
func NumericField(name string) Numeric {
	return Numeric{
		dynamoValueField{
			DynamoField{
				name:  name,
				_type: dN,
			},
		},
	}
}

/*NumericSetField ... A constructor for a numeric set dynamo field*/
func NumericSetField(name string) NumericSet {
	return NumericSet{
		dynamoSetField{
			dynamoCollectionField{
				DynamoField{
					name:  name,
					_type: dNS,
				},
			},
		},
	}
}

/*StringField ... A constructor for a string dynamo field*/
func StringField(name string) String {
	return String{
		dynamoValueField{
			DynamoField{
				name:  name,
				_type: dS,
			},
		},
	}
}

/*StringField ... A constructor for a string dynamo field*/
func BoolField(name string) Bool {
	return Bool{
		dynamoValueField{
			DynamoField{
				name:  name,
				_type: dBOOL,
			},
		},
	}
}

/*BinaryField ... A constructor for a binary dynamo field*/
func BinaryField(name string) Binary {
	return Binary{
		dynamoValueField{
			DynamoField{
				name:  name,
				_type: dB,
			},
		},
	}
}

/*BinarySetField ... A constructor for a binary set dynamo field*/
func BinarySetField(name string) BinarySet {
	return BinarySet{
		dynamoSetField{
			dynamoCollectionField{
				DynamoField{
					name:  name,
					_type: dBS,
				},
			},
		},
	}
}

/*StringSetField ... A constructor for a string set dynamo field*/
func StringSetField(name string) StringSet {
	return StringSet{
		dynamoSetField{
			dynamoCollectionField{
				DynamoField{
					name:  name,
					_type: dSS,
				},
			},
		},
	}
}

/*ListField ... A constructor for a list dynamo field*/
func ListField(name string) List {
	return List{
		dynamoListField{
			dynamoCollectionField{
				DynamoField{
					name:  name,
					_type: dL,
				},
			},
		},
	}
}

/*MapField ... A constructor for a map dynamo field*/
func MapField(name string) Map {
	return Map{
		dynamoMapField{
			DynamoField{
				name:  name,
				_type: dL,
			},
		},
	}
}

/*LocalSecondaryIndex ... Represents a dynamo local secondary index*/
type LocalSecondaryIndex struct {
	Name             string
	PartitionKey     DynamoFieldIFace
	SortKey          DynamoFieldIFace
	ProjectionType   string
	NonKeyAttributes []DynamoFieldIFace
}

/*GlobalSecondaryIndex ... Represents a dynamo global secondary index*/
type GlobalSecondaryIndex struct {
	Name             string
	PartitionKey     DynamoFieldIFace
	RangeKey         DynamoFieldIFace //Optional param. If no range key set to EmptyField
	ProjectionType   string
	NonKeyAttributes []DynamoFieldIFace
	ReadUnits        int64
	WriteUnits       int64
}

/*KeyValue ... A Key Value struct for use in GetItem and BatchWriteItem queries*/
type KeyValue struct {
	PartitionKey interface{}
	RangeKey     interface{}
}

type TableName string
type Keys *dynamodb.KeysAndAttributes

type dynamoResult struct {
	err error
}

func (r *dynamoResult) Error() error {
	return r.err
}

func (r *dynamoResult) ConditionalCheckFailed() (b bool) {
	if err := r.Error(); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				b = true
			default:
				b = false
			}

		}
	}
	return
}

/***************************************************************************************/
/************************************** GetItem ****************************************/
/***************************************************************************************/
type getInput dynamodb.GetItemInput
type getOutput struct {
	*dynamoResult
	*dynamodb.GetItemOutput
}

/*GetItem Primary constructor for creating a  get item query*/
func (table DynamoTable) GetItem(key KeyValue) *getInput {
	q := getInput(dynamodb.GetItemInput{})
	q.TableName = &table.Name
	appendAttribute(&q.Key, table.PartitionKey.Name(), key.PartitionKey)
	if !table.RangeKey.IsEmpty() {
		appendAttribute(&q.Key, table.RangeKey.Name(), key.RangeKey)
	}
	return &q
}

/*SetConsistentRead ... */
func (d *getInput) SetConsistentRead(c bool) *getInput {
	d.ConsistentRead = &c
	return d
}

func (d *getInput) SetProjectionExpression(exp string) *getInput {
	d.ProjectionExpression = &exp
	return d
}

func (d *getInput) Build() *dynamodb.GetItemInput {
	r := dynamodb.GetItemInput(*d)
	r.ReturnConsumedCapacity = aws.String("INDEXES")
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
func (d *getInput) ExecuteWith(ctx context.Context, dynamo DynamoDBIFace, opts ...request.Option) (out *getOutput) {

	o, err := dynamo.GetItemWithContext(ctx, d.Build(), opts...)
	dr := &dynamoResult{
		err,
	}
	out = &getOutput{
		dr,
		o,
	}

	return
}

func (o *getOutput) Result(item interface{}) (err error) {
	err = o.Error()
	if o.GetItemOutput == nil || err != nil || item == nil {
		return
	}
	return deserializeTo(o.Item, item)
}

/***************************************************************************************/
/************************************** BatchGetItem ***********************************/
/***************************************************************************************/
type batchGetInput struct {
	input *[]*dynamodb.BatchGetItemInput

	consistentRead bool
	/*A set of mutational operations that might error out, i.e. not pure, and therefore not conducive to a fluent dsl*/
	delayedFunctions []func() error
}
type batchGetOutput struct {
	*dynamoResult
	results []*dynamodb.BatchGetItemOutput
}

/*BatchGetItem represents dynamo batch get item call*/
func (table DynamoTable) BatchGetItem(items ...KeyValue) *batchGetInput {
	/*Delay the attribute value construction, until Build time*/
	input := &[]*dynamodb.BatchGetItemInput{}
	delayed := func() error {

		k := make(map[string]*dynamodb.KeysAndAttributes)
		keysAndAttribs := &dynamodb.KeysAndAttributes{}
		k[table.Name] = keysAndAttribs
		ss := []map[string]*dynamodb.KeysAndAttributes{k}

		for i, kv := range items {

			if (i-1)%100 == 99 {
				k = make(map[string]*dynamodb.KeysAndAttributes)
				ss = append(ss, k)

				keysAndAttribs = &dynamodb.KeysAndAttributes{}
				k[table.Name] = keysAndAttribs
			}

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

			(*keysAndAttribs).Keys = append((*keysAndAttribs).Keys, attributes)

		}

		for _, m := range ss {
			(*input) = append(*input, &dynamodb.BatchGetItemInput{RequestItems: m})
		}

		return nil
	}

	q := &batchGetInput{
		input:            input,
		delayedFunctions: []func() error{delayed},
	}

	return q
}

func (d *batchGetInput) Build() (input []*dynamodb.BatchGetItemInput, err error) {
	for _, function := range d.delayedFunctions {
		err = function()
		if err != nil {
			return
		}
	}
	input = *(d.input)
	for _, i := range input {
		i.ReturnConsumedCapacity = aws.String("INDEXES")

		// set read consistency on individual items.
		// this cannot be done in a delayedFunction because it depends on the context
		// of the batchGetInput items.
		for _, a := range i.RequestItems {
			a.ConsistentRead = &d.consistentRead
		}
	}

	return
}

/*SetConsistentRead ... */
func (d *batchGetInput) SetConsistentRead(c bool) *batchGetInput {
	d.consistentRead = c
	return d
}

/**
 ** ExecuteWith ... Execute a dynamo BatchGetItem call with a passed in dynamodb instance and next item pointer
 ** dynamo - The underlying dynamodb api
 **
 */
func (d *batchGetInput) ExecuteWith(ctx context.Context, dynamo DynamoDBIFace, opts ...request.Option) (out *batchGetOutput) {
	out = &batchGetOutput{
		dynamoResult: &dynamoResult{},
	}

	var input []*dynamodb.BatchGetItemInput

	if input, out.err = d.Build(); out.err != nil {
		return
	}

	for _, bg := range input {
		retry := 0
	Execute:
		var result *dynamodb.BatchGetItemOutput
		if result, out.err = dynamo.BatchGetItemWithContext(ctx, bg, opts...); out.err != nil {
			return
		}
		out.results = append(out.results, result)

		if result.UnprocessedKeys != nil && len(result.UnprocessedKeys) > 0 {
			bg.RequestItems = result.UnprocessedKeys
			retry++
			goto Execute
		}
	}

	return
}

/** Results ... Deserialize the results using a user provided target object generator function
 ** nextItem - The item pointer function, which is called on each new object returned from dynamodb. The function should
 ** 		   store each item in an array before returning.
 **/

func (o *batchGetOutput) Results(nextItem func() interface{}) (err error) {
	err = o.Error()
	if o.Error() != nil || nextItem == nil {
		return
	}
	for _, result := range o.results {
		for _, items := range result.Responses {
			for _, av := range items {
				if o.err = deserializeTo(av, nextItem()); o.err != nil {
					return
				}
			}
		}
	}
	return
}

/***************************************************************************************/
/************************************** PutItem ****************************************/
/***************************************************************************************/
type putInput dynamodb.PutItemInput
type putOutput struct {
	*dynamodb.PutItemOutput
	*dynamoResult
}

/*PutItem represents dynamo put item call*/
func (table DynamoTable) PutItem(i interface{}) *putInput {
	q := putInput(dynamodb.PutItemInput{})
	q.TableName = &table.Name
	q.Item, _ = dynamodbattribute.MarshalMap(i)
	return &q
}

func (d *putInput) ReturnAllOld() *putInput {
	(*dynamodb.PutItemInput)(d).SetReturnValues("ALL_OLD")
	return d
}
func (d *putInput) ReturnNone() *putInput {
	(*dynamodb.PutItemInput)(d).SetReturnValues("NONE")
	return d
}
func (d *putInput) SetConditionExpression(c Expression) *putInput {
	s, n, m, _ := c.construct(1, true)
	d.ConditionExpression = &s

	d.ExpressionAttributeNames = n

	d.ExpressionAttributeValues = marshal(m)

	return d
}

func (d *putInput) Build() *dynamodb.PutItemInput {
	r := dynamodb.PutItemInput(*d)
	return &r
}

/**
 ** ExecuteWith ... Execute a dynamo PutItem call with a passed in dynamodb instance
 ** ctx - An instance of context
 ** dynamo - The underlying dynamodb api
 **
 */
func (d *putInput) ExecuteWith(ctx context.Context, dynamo DynamoDBIFace, opts ...request.Option) (out *putOutput) {
	out = &putOutput{
		dynamoResult: &dynamoResult{},
	}
	if result, err := dynamo.PutItemWithContext(ctx, d.Build(), opts...); err != nil {
		out.err = err
	} else {
		out.PutItemOutput = result
	}

	return
}

func (o *putOutput) Result(item interface{}) (err error) {
	err = o.Error()
	if err != nil || o.PutItemOutput == nil || item == nil {
		return
	}
	deserializeTo(o.PutItemOutput.Attributes, item)
	return
}

/***************************************************************************************/
/************************************** BatchWriteItem *********************************/
/***************************************************************************************/
type batchWriteInput struct {
	batches          []*dynamodb.BatchWriteItemInput
	table            DynamoTable
	delayedFunctions []func() error
}
type batchPutOutput struct {
	*dynamoResult
	results []*dynamodb.BatchWriteItemOutput
}

/*BatchWriteItem represents dynamo batch write item call*/
func (table DynamoTable) BatchWriteItem() *batchWriteInput {
	r := batchWriteInput{
		batches: []*dynamodb.BatchWriteItemInput{},
		table:   table,
	}
	return &r
}

func (d *batchWriteInput) writeItems(putOnly bool, items ...interface{}) *batchWriteInput {
	if len(items) <= 0 {
		return d
	}
	delayed := func() error {
		var batch *dynamodb.BatchWriteItemInput

		for _, item := range items {
			if batch == nil {
				batch = &dynamodb.BatchWriteItemInput{
					RequestItems: make(map[string][]*dynamodb.WriteRequest),
				}
				d.batches = append(d.batches, batch)
			}

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
			batch.RequestItems[d.table.Name] = append(batch.RequestItems[d.table.Name], write)

			if len(batch.RequestItems[d.table.Name]) >= 25 {
				batch = nil
			}
		}

		return nil
	}
	d.delayedFunctions = append(d.delayedFunctions, delayed)

	return d
}

func (d *batchWriteInput) PutItems(items ...interface{}) *batchWriteInput {
	d.writeItems(true, items...)
	return d
}
func (d *batchWriteInput) DeleteItems(keys ...KeyValue) *batchWriteInput {
	a := []interface{}{}
	for _, key := range keys {
		m := map[string]interface{}{}
		appendKeyInterface(&m, d.table, key)
		a = append(a, m)
	}
	d.writeItems(false, a...)
	return d
}

func (d *batchWriteInput) Build() (input []*dynamodb.BatchWriteItemInput, err error) {
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
 ** ctx - An instance of context
 ** dynamo - The underlying dynamodb api
 ** unprocessedItem - The item pointer function, which is called on each object returned from dynamodb that could not be processed.
 ** 				The function should store each item pointer in an array before returning.
 **
 */
func (d *batchWriteInput) ExecuteWith(ctx context.Context, dynamo DynamoDBIFace, opts ...request.Option) (out *batchPutOutput) {
	out = &batchPutOutput{
		dynamoResult: &dynamoResult{},
	}

	batches, err := d.Build()
	if err != nil {
		out.err = err
		return
	}
	for _, batch := range batches {
		result, err := dynamo.BatchWriteItemWithContext(ctx, batch, opts...)
		if err != nil {
			out.err = err
			return
		}
		out.results = append(out.results, result)
	}

	return
}

func (d *batchPutOutput) Results(unprocessedItem func() interface{}) (err error) {
	err = d.Error()
	if err != nil || d.results == nil || unprocessedItem == nil {
		return
	}
	for _, result := range d.results {
		for _, items := range result.UnprocessedItems {
			for _, item := range items {
				if err = deserializeTo(item.PutRequest.Item, unprocessedItem()); err != nil {
					d.err = err
					return
				}
			}
		}
	}
	return
}

/***************************************************************************************/
/*************************************** DeleteItem ************************************/
/***************************************************************************************/
type deleteItemInput dynamodb.DeleteItemInput
type deleteItemOutput struct {
	*dynamoResult
	*dynamodb.DeleteItemOutput
}

/*DeleteItemInput represents dynamo delete item call*/
func (table DynamoTable) DeleteItem(key KeyValue) *deleteItemInput {
	q := deleteItemInput(dynamodb.DeleteItemInput{})
	q.TableName = &table.Name
	appendKeyAttribute(&q.Key, table, key)
	return &q
}

func (d *deleteItemInput) ReturnAllOld() *deleteItemInput {
	(*dynamodb.DeleteItemInput)(d).SetReturnValues("ALL_OLD")
	return d
}

func (d *deleteItemInput) ReturnNone() *deleteItemInput {
	(*dynamodb.DeleteItemInput)(d).SetReturnValues("NONE")
	return d
}

func (d *deleteItemInput) SetConditionExpression(c Expression) *deleteItemInput {
	s, n, m, _ := c.construct(1, true)
	d.ConditionExpression = &s

	d.ExpressionAttributeNames = n

	if d.ExpressionAttributeValues == nil {
		d.ExpressionAttributeValues = marshal(m)
	} else {
		for k, v := range marshal(m) {
			d.ExpressionAttributeValues[k] = v
		}
	}

	return d
}

func (d *deleteItemInput) Build() *dynamodb.DeleteItemInput {
	r := dynamodb.DeleteItemInput(*d)
	return &r
}

/**
 ** ExecuteWith ... Execute a dynamo DeleteItem call with a passed in dynamodb instance
 ** ctx - An instance of context
 ** dynamo - The underlying dynamodb api
 **
 */
func (d *deleteItemInput) ExecuteWith(ctx context.Context, dynamo DynamoDBIFace, opts ...request.Option) (out *deleteItemOutput) {
	out = &deleteItemOutput{
		dynamoResult: &dynamoResult{},
	}
	result, err := dynamo.DeleteItemWithContext(ctx, d.Build(), opts...)
	if err != nil {
		out.err = err
		return
	}
	out.DeleteItemOutput = result
	return
}

func (o *deleteItemOutput) Result(item interface{}) (err error) {
	err = o.err
	if err != nil || o.DeleteItemOutput == nil || item == nil {
		return
	}
	if err = deserializeTo(o.DeleteItemOutput.Attributes, item); err != nil {
		o.err = err
	}
	return
}

/***************************************************************************************/
/*********************************** UpdateItem ****************************************/
/***************************************************************************************/
type UpdateInput struct {
	input            dynamodb.UpdateItemInput
	delayedFunctions []func() error
}

type UpdateOutput struct {
	*dynamodb.UpdateItemOutput
	*dynamoResult
}

/*UpdateInputItem represents dynamo batch get item call*/
func (table DynamoTable) UpdateItem(key KeyValue) *UpdateInput {
	q := &UpdateInput{input: dynamodb.UpdateItemInput{TableName: &table.Name}}
	appendKeyAttribute(&(q.input.Key), table, key)
	return q
}

func (d *UpdateInput) ReturnAllNew() *UpdateInput {
	d.input.SetReturnValues("ALL_NEW")
	return d
}

func (d *UpdateInput) ReturnAllOld() *UpdateInput {
	d.input.SetReturnValues("ALL_OLD")
	return d
}

func (d *UpdateInput) ReturnUpdatedNew() *UpdateInput {
	d.input.SetReturnValues("UPDATED_NEW")
	return d
}

func (d *UpdateInput) ReturnUpdatedOld() *UpdateInput {
	d.input.SetReturnValues("UPDATED_OLD")
	return d
}

func (d *UpdateInput) ReturnNone() *UpdateInput {
	d.input.SetReturnValues("NONE")
	return d
}

func (d *UpdateInput) SetConditionExpression(c Expression) *UpdateInput {
	delayed := func() error {
		s, n, m, _ := c.construct(1, true)
		d.input.ConditionExpression = &s

		d.input.ExpressionAttributeNames = n

		if d.input.ExpressionAttributeValues == nil {
			d.input.ExpressionAttributeValues = marshal(m)
		} else {
			for k, v := range marshal(m) {
				d.input.ExpressionAttributeValues[k] = v
			}
		}

		return nil
	}
	d.delayedFunctions = append(d.delayedFunctions, delayed)
	return d
}

func (d *UpdateInput) SetUpdateExpression(exprs ...*UpdateExpression) *UpdateInput {
	m := make(map[string]interface{})
	ms := make(map[string]string)

	c := uint(100)
	for _, expr := range exprs {
		s, mv, mr, nc := expr.f(c)
		c = nc
		for k, v := range mr {
			m[k] = v
		}
		if d.input.ExpressionAttributeNames == nil && len(mv) > 0 {
			d.input.ExpressionAttributeNames = mv
		} else {
			for k, v := range mv {
				d.input.ExpressionAttributeNames[k] = v
			}
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

	if d.input.ExpressionAttributeValues == nil {
		d.input.ExpressionAttributeValues = marshal(m)
	} else {
		for k, v := range marshal(m) {
			d.input.ExpressionAttributeValues[k] = v
		}
	}

	return d
}

func (d *UpdateInput) Build() (r *dynamodb.UpdateItemInput, err error) {

	for _, function := range d.delayedFunctions {
		err = function()
		if err != nil {
			return nil, err
		}
	}
	rr := dynamodb.UpdateItemInput((*d).input)
	return &rr, err
}

/**
 ** ExecuteWith ... Execute a dynamo BatchGetItem call with a passed in dynamodb instance
 ** ctx - an instance of context
 ** dynamo - The underlying dynamodb api
 **
 */
func (d *UpdateInput) ExecuteWith(ctx context.Context, dynamo DynamoDBIFace, opts ...request.Option) (out *UpdateOutput) {
	out = &UpdateOutput{
		dynamoResult: &dynamoResult{},
	}
	input, err := d.Build()
	if err != nil {
		out.err = err
		return
	}
	out.UpdateItemOutput, out.err = dynamo.UpdateItemWithContext(ctx, input, opts...)

	return
}
func (o *UpdateOutput) Result(item interface{}) (err error) {
	err = o.err
	if err != nil || o.UpdateItemOutput == nil || item == nil {
		return
	}
	if err := deserializeTo(o.UpdateItemOutput.Attributes, item); err != nil {
		o.err = err
	}
	return
}

/***************************************************************************************/
/********************************************** Query **********************************/
/***************************************************************************************/
type QueryInput struct {
	*dynamodb.QueryInput
	pageSize         *int64
	capacityHandlers []func(*dynamodb.ConsumedCapacity)
}

type QueryOutput struct {
	*dynamoResult
	outputFunc func() (*dynamodb.QueryOutput, error)
	limit      *int64
	ctx        context.Context
}

/*QueryInput represents dynamo batch get item call*/
func (table DynamoTable) Query(partitionKeyCondition KeyCondition, rangeKeyCondition *KeyCondition) *QueryInput {
	q := QueryInput{
		QueryInput: &dynamodb.QueryInput{},
	}

	var e Expression
	if rangeKeyCondition != nil {
		e = And(partitionKeyCondition, *rangeKeyCondition)
	} else {
		e = partitionKeyCondition
	}

	s, n, m, _ := e.construct(0, true)
	q.TableName = &table.Name
	q.KeyConditionExpression = &s
	q.ExpressionAttributeNames = n
	q.ExpressionAttributeValues = marshal(m)

	return &q
}

func (d *QueryInput) SetConsistentRead(c bool) *QueryInput {
	(*d).ConsistentRead = &c
	return d
}
func (d *QueryInput) SetAttributesToGet(fields []DynamoField) *QueryInput {
	a := make([]*string, len(fields))
	for i, f := range fields {
		v := f.Name()
		a[i] = &v
	}
	(*d).AttributesToGet = a
	return d
}

func (d *QueryInput) SetLimit(limit int) *QueryInput {
	s := int64(limit)
	d.Limit = &s
	return d
}

func (d *QueryInput) SetPageSize(pageSize int) *QueryInput {
	ps := int64(pageSize)
	d.pageSize = &ps
	return d
}

func (d *QueryInput) SetScanForward(forward bool) *QueryInput {
	d.ScanIndexForward = &forward
	return d
}

func (d *QueryInput) WithConsumedCapacityHandler(f func(*dynamodb.ConsumedCapacity)) *QueryInput {
	d.ReturnConsumedCapacity = aws.String("INDEXES")
	d.capacityHandlers = append(d.capacityHandlers, f)
	return d
}

func (d *QueryInput) SetFilterExpression(c Expression) *QueryInput {
	s, n, m, _ := c.construct(1, true)
	d.FilterExpression = &s

	d.ExpressionAttributeNames = n
	if d.ExpressionAttributeValues == nil {
		d.ExpressionAttributeValues = marshal(m)
	} else {
		for k, v := range marshal(m) {
			d.ExpressionAttributeValues[k] = v
		}
	}

	return d
}

func (d *QueryInput) SetLocalIndex(idx LocalSecondaryIndex) *QueryInput {
	d.IndexName = &idx.Name
	return d
}

func (d *QueryInput) SetGlobalIndex(idx GlobalSecondaryIndex) *QueryInput {
	d.IndexName = &idx.Name
	return d
}

func (d *QueryInput) Build() *dynamodb.QueryInput {
	r := dynamodb.QueryInput(*d.QueryInput)
	if d.pageSize != nil {
		r.Limit = d.pageSize
	}

	return &r
}

/**
 ** StreamWith ... Execute a dynamo Stream call with a passed in dynamodb instance and next item pointer
 ** ctx - An instance of context
 ** dynamo - The underlying dynamodb api
 ** nextItem - The item pointer which is copied and hydrated on every item. The function SHOULD NOT
 ** 		   store each item. It should simply return an empty struct pointer. Each of which is hydrated and pused on the
 ** 			returned channel.
 **
 */

func (d *QueryInput) ExecuteWith(ctx context.Context, db DynamoDBIFace, opts ...request.Option) (out *QueryOutput) {

	out = &QueryOutput{
		dynamoResult: &dynamoResult{},
		ctx:          ctx,
		limit:        d.Limit,
	}

	q := d.Build()

	out.outputFunc = func() (o *dynamodb.QueryOutput, err error) {
		if q == nil {
			return
		}
		o, err = db.QueryWithContext(ctx, q, opts...)
		if err != nil {
			out.err = err
			return
		}
		for _, handler := range d.capacityHandlers {
			handler(o.ConsumedCapacity)
		}

		if o.LastEvaluatedKey != nil {
			q.ExclusiveStartKey = o.LastEvaluatedKey
		} else {
			q = nil
		}
		return
	}

	return

}

func (o *QueryOutput) Results(next func() interface{}) (err error) {
	err = o.err
	if err != nil || o.outputFunc == nil {
		return
	}
	var count int64
	for {
		var out *dynamodb.QueryOutput
		if out, err = o.outputFunc(); err != nil {
			o.err = err
			return
		} else if out == nil || len(out.Items) <= 0 {
			return
		}

		for _, av := range out.Items {
			if o.limit != nil && count >= *o.limit {
				return
			}
			count++
			item := next()
			if err = deserializeTo(av, item); err != nil {
				o.err = err
				return
			}
		}

	}
	return
}

func (o *QueryOutput) StreamWithChannel(channel interface{}) (errChan chan error) {
	t := reflect.TypeOf(channel).Elem()
	isPtr := t.Kind() == reflect.Ptr
	if isPtr {
		t = t.Elem()
	}
	vc := reflect.ValueOf(channel)
	errChan = make(chan error)
	count := int64(0)
	go func() {
		defer close(errChan)
		defer vc.Close()

		for {
			out, err := o.outputFunc()
			if err != nil {
				errChan <- err
			} else if out == nil || len(out.Items) <= 0 {
				return
			}
			for _, av := range out.Items {
				if o.limit != nil && count >= *o.limit {
					return
				}
				item := reflect.New(t).Interface()
				count++
				if err := deserializeTo(av, item); err != nil {
					errChan <- err
				} else {
					value := reflect.ValueOf(item)
					if !isPtr {
						value = reflect.Indirect(value)
					}
					c := reflect.SelectCase{
						Dir:  reflect.SelectSend,
						Chan: vc,
						Send: value,
					}
					d := reflect.SelectCase{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(o.ctx.Done()),
						Send: reflect.Value{},
					}
					if idx, _, _ := reflect.Select([]reflect.SelectCase{c, d}); idx == 1 {
						// ctx done
						return
					}
				}
			}
		}
	}()

	return
}

/***************************************************************************************/
/********************************************** Scan **********************************/
/***************************************************************************************/
type ScanInput struct {
	*dynamodb.ScanInput
	pageSize *int64
}

type ScanOutput struct {
	*dynamoResult
	outputFunc func() (*dynamodb.ScanOutput, error)
	Error      error
	limit      *int64
	ctx        context.Context
}

/*ScanOutput represents dynamo scan item call*/
func (table DynamoTable) Scan() (q *ScanInput) {

	q = &ScanInput{
		ScanInput: &dynamodb.ScanInput{},
	}

	q.TableName = &table.Name
	return
}

func (d *ScanInput) SetConsistentRead(c bool) *ScanInput {
	(*d).ConsistentRead = &c
	return d
}
func (d *ScanInput) SetAttributesToGet(fields []DynamoField) *ScanInput {
	a := make([]*string, len(fields))
	for i, f := range fields {
		v := f.Name()
		a[i] = &v
	}
	(*d).AttributesToGet = a
	return d
}

func (d *ScanInput) SetLimit(limit int) *ScanInput {
	s := int64(limit)
	d.Limit = &s
	return d
}

func (d *ScanInput) SetPageSize(pageSize int) *ScanInput {
	ps := int64(pageSize)
	d.pageSize = &ps
	return d
}

func (d *ScanInput) SetFilterExpression(c Expression) *ScanInput {
	s, n, m, _ := c.construct(1, true)
	d.FilterExpression = &s

	d.ExpressionAttributeNames = n
	if d.ExpressionAttributeValues == nil {
		d.ExpressionAttributeValues = marshal(m)
	} else {
		for k, v := range marshal(m) {
			d.ExpressionAttributeValues[k] = v
		}
	}

	return d
}

func (d *ScanInput) SetLocalIndex(idx LocalSecondaryIndex) *ScanInput {
	d.IndexName = &idx.Name
	return d
}

func (d *ScanInput) SetGlobalIndex(idx GlobalSecondaryIndex) *ScanInput {
	d.IndexName = &idx.Name
	return d
}

func (d *ScanInput) Build() *dynamodb.ScanInput {
	r := dynamodb.ScanInput(*d.ScanInput)
	if d.pageSize != nil {
		r.Limit = d.pageSize
	}
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
func (d *ScanInput) ExecuteWith(ctx context.Context, db DynamoDBIFace, opts ...request.Option) (out *ScanOutput) {

	out = &ScanOutput{
		dynamoResult: &dynamoResult{},
		ctx:          ctx,
		limit:        d.Limit,
	}

	q := d.Build()

	out.outputFunc = func() (o *dynamodb.ScanOutput, err error) {
		if q == nil {
			return
		}
		o, err = db.ScanWithContext(ctx, q, opts...)
		if err != nil {
			out.err = err
			return
		}

		if o.LastEvaluatedKey != nil {
			q.ExclusiveStartKey = o.LastEvaluatedKey
		} else {
			q = nil
		}
		return
	}

	return

}

func (o *ScanOutput) Results(next func() interface{}) (err error) {
	err = o.Error
	if err != nil || o.outputFunc == nil {
		return
	}
	var count int64
	for {
		var out *dynamodb.ScanOutput
		if out, err = o.outputFunc(); err != nil {
			o.err = err
			return
		} else if out == nil || len(out.Items) <= 0 {
			return
		}

		for _, av := range out.Items {
			if o.limit != nil && count >= *o.limit {
				return
			}
			count++
			item := next()
			o.err = deserializeTo(av, item)
			if err = o.err; err != nil {
				return
			}
		}

	}
	return
}

func (o *ScanOutput) StreamWithChannel(channel interface{}) (errChan chan error) {
	t := reflect.TypeOf(channel).Elem()
	isPtr := t.Kind() == reflect.Ptr
	if isPtr {
		t = t.Elem()
	}
	vc := reflect.ValueOf(channel)
	errChan = make(chan error)
	count := int64(0)
	go func() {
		defer close(errChan)
		defer vc.Close()

		for {
			out, err := o.outputFunc()
			if err != nil {
				errChan <- err
			} else if out == nil || len(out.Items) <= 0 {
				return
			}
			for _, av := range out.Items {
				if o.limit != nil && count >= *o.limit {
					return
				}
				item := reflect.New(t).Interface()
				count++
				if err := deserializeTo(av, item); err != nil {
					errChan <- err
				} else {
					value := reflect.ValueOf(item)
					if !isPtr {
						value = reflect.Indirect(value)
					}
					c := reflect.SelectCase{
						Dir:  reflect.SelectSend,
						Chan: vc,
						Send: value,
					}
					d := reflect.SelectCase{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(o.ctx.Done()),
						Send: reflect.Value{},
					}
					if idx, _, _ := reflect.Select([]reflect.SelectCase{c, d}); idx == 1 {
						// ctx done
						return
					}
				}
			}
		}
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

	// add GlobalSecondaryIndexes
	if len(table.GlobalSecondaryIndexes) > 0 {
		for _, gsi := range table.GlobalSecondaryIndexes {
			c = *c.WithGlobalSecondaryIndex(gsi)
		}
	}

	// add LocalSecondaryIndexes
	if len(table.LocalSecondaryIndexes) > 0 {
		for _, lsi := range table.LocalSecondaryIndexes {
			c = *c.WithLocalSecondaryIndex(lsi)
		}
	}

	return &c
}

func (d *createTable) WithLocalSecondaryIndex(lsi LocalSecondaryIndex) *createTable {
	// handle projection types and NonKeyAttributes
	var pt *string
	var nka []*string
	if lsi.ProjectionType == "" {
		pt = aws.String(ProjectionTypeALL)
	} else {
		// ALL, INCLUDE, KEYS_ONLY
		pt = aws.String(lsi.ProjectionType)
		if lsi.ProjectionType == ProjectionTypeINCLUDE {
			for _, key := range lsi.NonKeyAttributes {
				newAttr := &dynamodb.AttributeDefinition{
					AttributeName: aws.String(key.Name()),
					AttributeType: aws.String(key.Type()),
				}
				d.AttributeDefinitions = append(d.AttributeDefinitions, newAttr)
				nka = append(nka, aws.String(key.Name()))
			}
		}
	}

	// populate missing AttributeDefinitions
	pk := &dynamodb.AttributeDefinition{
		AttributeName: aws.String(lsi.PartitionKey.Name()),
		AttributeType: aws.String(lsi.PartitionKey.Type()),
	}
	rk := &dynamodb.AttributeDefinition{
		AttributeName: aws.String(lsi.SortKey.Name()),
		AttributeType: aws.String(lsi.SortKey.Type()),
	}
	d.AttributeDefinitions = append(d.AttributeDefinitions, pk)
	d.AttributeDefinitions = append(d.AttributeDefinitions, rk)

	// create lsi obj
	dynamoLsi := dynamodb.LocalSecondaryIndex{
		IndexName: &lsi.Name,
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(lsi.PartitionKey.Name()),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(lsi.SortKey.Name()),
				KeyType:       aws.String("RANGE"),
			},
		},
		Projection: &dynamodb.Projection{
			ProjectionType:   pt,
			NonKeyAttributes: nka,
		},
	}

	// append lsi to *createTable
	d.LocalSecondaryIndexes = append(d.LocalSecondaryIndexes, &dynamoLsi)
	return d
}

func (d *createTable) WithGlobalSecondaryIndex(gsi GlobalSecondaryIndex) *createTable {
	// handle projection types and NonKeyAttributes
	var pt *string
	var nka []*string
	if gsi.ProjectionType == "" {
		pt = aws.String(ProjectionTypeALL)
	} else {
		// ALL, INCLUDE, KEYS_ONLY
		pt = aws.String(gsi.ProjectionType)
		if gsi.ProjectionType == ProjectionTypeINCLUDE {
			for _, key := range gsi.NonKeyAttributes {
				newAttr := &dynamodb.AttributeDefinition{
					AttributeName: aws.String(key.Name()),
					AttributeType: aws.String(key.Type()),
				}
				d.AttributeDefinitions = append(d.AttributeDefinitions, newAttr)
				nka = append(nka, aws.String(key.Name()))
			}
		}
	}

	// setup default provisioning
	var gsir *int64
	var gsiw *int64

	if gsi.ReadUnits != 0 {
		gsir = &gsi.ReadUnits
	} else {
		gsir = aws.Int64(10)
	}
	if gsi.WriteUnits != 0 {
		gsiw = &gsi.WriteUnits
	} else {
		gsiw = aws.Int64(10)
	}

	// populate missing AttributeDefinitions
	pk := &dynamodb.AttributeDefinition{
		AttributeName: aws.String(gsi.PartitionKey.Name()),
		AttributeType: aws.String(gsi.PartitionKey.Type()),
	}
	d.AttributeDefinitions = append(d.AttributeDefinitions, pk)
	
	if !gsi.RangeKey.IsEmpty() {
		rk := &dynamodb.AttributeDefinition{
			AttributeName: aws.String(gsi.RangeKey.Name()),
			AttributeType: aws.String(gsi.RangeKey.Type()),
		}
		d.AttributeDefinitions = append(d.AttributeDefinitions, rk)
	}

	// create gsi obj
	dynamoGsi := dynamodb.GlobalSecondaryIndex{
		IndexName: &gsi.Name,
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(gsi.PartitionKey.Name()),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(gsi.RangeKey.Name()),
				KeyType:       aws.String("RANGE"),
			},
		},
		Projection: &dynamodb.Projection{
			ProjectionType:   pt,
			NonKeyAttributes: nka,
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  gsir,
			WriteCapacityUnits: gsiw,
		},
	}

	// append gsi to *createTable
	d.GlobalSecondaryIndexes = append(d.GlobalSecondaryIndexes, &dynamoGsi)
	return d
}

func (d *createTable) Build() *dynamodb.CreateTableInput {
	r := dynamodb.CreateTableInput(*d)
	defer time.Sleep(time.Duration(500) * time.Millisecond)
	return &r
}

func (d *createTable) ExecuteWith(ctx context.Context, dynamo DynamoDBIFace, opts ...request.Option) error {
	defer time.Sleep(time.Duration(500) * time.Millisecond)
	_, err := dynamo.CreateTableWithContext(ctx, d.Build(), opts...)
	return err
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

func (d *deleteTable) ExecuteWith(ctx context.Context, dynamo DynamoDBIFace, opts ...request.Option) error {
	defer time.Sleep(time.Duration(500) * time.Millisecond)
	_, err := dynamo.DeleteTableWithContext(ctx, d.Build(), opts...)
	return err
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
		*m = make(DynamoDBValue)
	}
	v, err := dynamodbattribute.Marshal(value)
	if err == nil {
		(*m)[key] = v
	}
	return
}
