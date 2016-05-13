# Domino
Typesafe DynamoDB query DSL for Go


Features:  
	* Fully typesafe fluent syntax DSL  
	* Full Condition and Filter Expression functionality  
	* Static schema definition  
	* Streaming results for Query, Scan and BatchGet operations  


```go

sess := session.New(config)
dynamo := dynamodb.New(sess)

//Define your table schema statically
type UserTable struct {
	DynamoTable
	emailField    domino.String
	passwordField domino.String

	registrationDate domino.Numeric
	loginCount       domino.Numeric
	lastLoginDate    domino.Numeric
	vists            domino.NumericSet
	preferences      dynamoFieldMap
	nameField        domino.String
	lastNameField    domino.String

	registrationDateIndex domino.LocalSecondaryIndex
	nameGlobalIndex       domino.GlobalSecondaryIndex
}

type User struct {
	Email       string            `json:"email"`
	Password    string            `json:"password"`
	Visits      []int64           `json:"visits"`
	LoginCount  int               `json:"loginCount"`
	RegDate     int64             `json:"registrationDate"`
	Preferences map[string]string `json:"preferences"`
}

func NewUserTable() MyTable {
	pk := domino.StringField("email")
	rk := domino.StringField("password")
	firstName := domino.StringField("firstName")
	lastName := domino.StringField("lastName")
	reg := domino.NumericField("registrationDate")
	return MyTable{
		DynamoTable{
			Name:         "mytable",
			PartitionKey: pk,
			RangeKey:     rk,
		},
		pk,  //email
		rk,  //password
		reg, //registration
		domino.NumericField("loginCount"),
		domino.NumericField("lastLoginDate"),
		domino.NumericFieldSet("visits"),
		domino.MapField("preferences"),
		firstName,
		lastName,
		domino.LocalSecondaryIndex{"registrationDate-index", reg},
		domino.GlobalSecondaryIndex{"name-index", firstName, lastName},
	}
}

table := NewMyTable() 

```

Use a fluent style DSL to interact with your table. All DynamoDB data operations are supported


Put Item
```go
p := table.
	PutItem(User{"naveen@email.com","password"}).
	SetConditionExpression(
		table.PartitionKey.NotExists()
	).
	Build()
r, err := dynamo.PutItem(q)
```

GetItem
```go
q = table.
	GetItem(KeyValue{"naveen@email.com", "password"}).
	SetConsistentRead(true).
	Build()  //This is type GetItemInput
r, err = dynamo.GetItem(q, &User{}).ExecuteWith(dynamo)

```


Update Item
```go
q := table.
	UpdateItem(KeyValue{"naveen@email.com", "password"}).
	SetUpdateExpression(
		table.loginCount.Increment(1),
		table.lastLoginDate.SetField(time.Now().UnixNano(), false),
		table.registrationDate.SetField(time.Now().UnixNano(), true),
		table.vists.RemoveElemIndex(0),
		table.preferences.RemoveKey("update_email"),
	).Build()
r, err = dynamo.UpdateItem(q)	
```

Query
```go
p = table.lastNameField.Equals("Gattu")
q = table.
	Query(
		table.nameField.Equals("naveen"),
		&p,
	).
	SetLimit(100).
	SetScanForward(true).
	SetLocalIndex(table.registrationDateIndex).
	Build() 

r, err = dynamo.Query(q)	
```

Batch Get Item
```go
q = table.
	BatchGetItem(
		KeyValue{"naveen@email.com", "password"},
		KeyValue{"joe@email.com", "password"},
	).
	SetConsistentRead(true).
	Build()
r, err = dynamo.BatchGetItem(q)	
```


Fully typesafe condition expression and filter expression support.
```go
expr := Or(
		table.lastNameField.Contains("gattu"),
		Not(table.registrationDate.Contains(12345)),
		And(
			table.visits.Size(lte, 25),
			table.nameField.Size(gte, 25),
		),		
		table.lastLoginDate.LessThanOrEq(time.Now().UnixNano()),		
	)
q = table.
	Query(
		table.nameField.Equals("naveen"),
		&p,
	).
	SetFilterExpression(expr).
	Build()


```

Streaming Results 

```go

	/*Query*/

	p := table.passwordField.BeginsWith("password")
	q := table.
		Query(
			table.emailField.Equals("naveen@email.com"),
			&p,
		).
		SetLimit(100).
		SetScanForward(true)

	channel, errChan := q.ExecuteWith(db, &User{})

	for {
		select {
		case u, ok := <-channel:
			if ok {
				fmt.Println(u.(*User))
			}
		case err = <-errChan:

		}
	}

```


```go
	
	/*Scan*/

	p := table.passwordField.BeginsWith("password")
	q := table.Scan().SetLimit(100).		

	channel, errChan := q.ExecuteWith(db, &User{})

	for {
		select {
		case u, ok := <-channel:
			if ok {
				fmt.Println(u.(*User))
			}
		case err = <-errChan:

		}
	}

```

