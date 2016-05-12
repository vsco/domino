package domino

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"net/http"
	"strconv"
	"testing"
	"time"
)

const localDynamoHost = "http://127.0.0.1:8080"

type UserTable struct {
	DynamoTable
	emailField    dynamoFieldString
	passwordField dynamoFieldString

	registrationDate dynamoFieldNumeric
	loginCount       dynamoFieldNumeric
	lastLoginDate    dynamoFieldNumeric
	visits           dynamoFieldNumericSet
	preferences      dynamoFieldMap
	nameField        dynamoFieldString
	lastNameField    dynamoFieldString

	registrationDateIndex LocalSecondaryIndex
	nameGlobalIndex       GlobalSecondaryIndex
}

type User struct {
	Email       string            `json:"email"`
	Password    string            `json:"password"`
	Visits      []int64           `json:"visits"`
	LoginCount  int               `json:"loginCount"`
	RegDate     int64             `json:"registrationDate"`
	Preferences map[string]string `json:"preferences"`
}

func NewUserTable() UserTable {
	pk := DynamoFieldString("email")
	rk := DynamoFieldString("password")
	firstName := DynamoFieldString("firstName")
	lastName := DynamoFieldString("lastName")
	reg := DynamoFieldNumeric("registrationDate")
	return UserTable{
		DynamoTable{
			Name:         "dev-ore-feed",
			PartitionKey: pk,
			RangeKey:     rk,
		},
		pk,  //email
		rk,  //password
		reg, //registration
		DynamoFieldNumeric("loginCount"),
		DynamoFieldNumeric("lastLoginDate"),
		DynamoFieldNumericSet("visits"),
		DynamoFieldMap("preferences"),
		firstName,
		lastName,
		LocalSecondaryIndex{"registrationDate-index", reg},
		GlobalSecondaryIndex{"name-index", firstName, lastName},
	}
}

func NewDB() DynamoDBIFace {
	sess := session.New()
	region := "us-west-2"
	config := &aws.Config{
		Credentials: credentials.NewStaticCredentials("123", "123", ""),
		Region:      &region,
	}
	sess.Config = config.WithEndpoint(localDynamoHost).WithHTTPClient(http.DefaultClient)

	return dynamodb.New(sess)
}

func TestGetItem(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	err := table.CreateTable().ExecuteWith(db)
	defer table.DeleteTable().ExecuteWith(db)

	item := User{Email: "naveen@email.com", Password: "password"}
	err = table.PutItem(item).ExecuteWith(db)
	assert.Nil(t, err)

	user := User{}
	err = table.GetItem(KeyValue{"naveen@email.com", "password"}).ExecuteWith(db, &user)
	assert.Nil(t, err)
	assert.Equal(t, User{Email: "naveen@email.com", Password: "password"}, user)
}

func TestBatchPutItem(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	err := table.CreateTable().ExecuteWith(db)
	defer table.DeleteTable().ExecuteWith(db)

	q := table.
		BatchWriteItem().
		PutItems(
			User{Email: "bob@email.com", Password: "password"},
			User{Email: "joe@email.com", Password: "password"},
			User{Email: "alice@email.com", Password: "password"},
		).
		DeleteItems(
			KeyValue{"naveen@email.com", "password"},
		)
	unprocessed := []*User{}
	err = q.ExecuteWith(db, func() interface{} {
		user := User{}
		unprocessed = append(unprocessed, &user)
		return &user
	})

	assert.Empty(t, unprocessed)
	assert.Nil(t, err)

	g := table.
		BatchGetItem(
			KeyValue{"bob@email.com", "password"},
			KeyValue{"joe@email.com", "password"},
			KeyValue{"alice@email.com", "password"},
		)

	users := []*User{}
	g.ExecuteWith(db, func() interface{} {
		user := User{}
		users = append(users, &user)
		return &user
	})

	assert.NotEmpty(t, users)
}

func TestUpdateItem(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	err := table.CreateTable().ExecuteWith(db)
	defer table.DeleteTable().ExecuteWith(db)

	item := User{Email: "naveen@email.com", Password: "password", Visits: []int64{time.Now().UnixNano()}}
	q := table.PutItem(item)
	err = q.ExecuteWith(db)

	u := table.
		UpdateItem(KeyValue{"naveen@email.com", "password"}).
		SetUpdateExpression(
			table.loginCount.Increment(1),
			table.lastLoginDate.SetField(time.Now().UnixNano(), false),
			table.registrationDate.SetField(time.Now().UnixNano(), true),
			table.visits.Append(time.Now().UnixNano()),
			table.preferences.RemoveKey("update_email"),
		)

	err = u.ExecuteWith(db)
	assert.Nil(t, err)
	g := table.GetItem(KeyValue{"naveen@email.com", "password"})
	var user User
	err = g.ExecuteWith(db, &user)

	assert.NotNil(t, user)

}

func TestPutItem(t *testing.T) {
	table := NewUserTable()
	db := NewDB()

	err := table.CreateTable().ExecuteWith(db)
	defer table.DeleteTable().ExecuteWith(db)

	assert.Nil(t, err)

	item := User{Email: "joe@email.com", Password: "password"}
	q := table.PutItem(item)
	err = q.ExecuteWith(db)

	v := table.
		UpdateItem(
			KeyValue{"joe@email.com", "password"},
		).
		SetUpdateExpression(
			table.loginCount.Increment(1),
			table.registrationDate.SetField(time.Now().UnixNano(), false),
		)
	err = v.ExecuteWith(db)

	assert.Nil(t, err)

	g := table.GetItem(KeyValue{"joe@email.com", "password"})
	var user User
	err = g.ExecuteWith(db, &user)

	assert.NotNil(t, user)

}
func TestExpressions(t *testing.T) {
	table := NewUserTable()
	db := NewDB()

	err := table.CreateTable().ExecuteWith(db)
	defer table.DeleteTable().ExecuteWith(db)

	expr := Or(
		table.registrationDate.BeginsWith("t"),
		table.lastNameField.Contains(strconv.Itoa(25)),
		Not(table.registrationDate.Contains("t")),
		And(
			table.registrationDate.Size(lte, 25),
			table.nameField.Size(gte, 25),
		),
		table.registrationDate.Equals("test"),
		table.registrationDate.LessThanOrEq("test"),
		table.registrationDate.Between("0", "1"),
		table.registrationDate.In("0", "1"),
	)

	p := table.passwordField.Equals("password")
	q := table.
		Query(
			table.emailField.Equals("naveen@email.com"),
			&p,
		).
		SetLimit(100).
		SetScanForward(true).
		SetFilterExpression(expr)

	err = q.ExecuteWith(db, func() interface{} {
		u := User{}
		return &u
	})
	assert.Nil(t, err)
}

func TestDynamoQuery(t *testing.T) {

	table := NewUserTable()
	db := NewDB()

	err := table.CreateTable().ExecuteWith(db)
	defer table.DeleteTable().ExecuteWith(db)

	assert.Nil(t, err)

	userMe := User{Email: "naveen@email.com", Password: "password"}
	items := []interface{}{userMe}
	for i := 0; i < 100; i++ {
		e := fmt.Sprintf("email_%v@email.com", i)
		items = append(items, User{Email: e, Password: "password"})
	}

	ui := []*User{}
	w := table.BatchWriteItem().PutItems(items...)
	// fmt.Println(w.Build())

	err = w.ExecuteWith(db, func() interface{} {
		u := User{}
		ui = append(ui, &u)
		return &u
	})

	assert.Nil(t, err)

	assert.Empty(t, ui)

	p := table.passwordField.Equals("password")
	q := table.
		Query(
			table.emailField.Equals("naveen@email.com"),
			&p,
		).
		SetLimit(100).
		SetScanForward(true).
		SetFilterExpression(And(table.passwordField.Exists()))

	results := []*User{}
	err = q.ExecuteWith(db, func() interface{} {
		user := User{}
		results = append(results, &user)
		return &user
	})

	assert.Nil(t, err)

	assert.Equal(t, []*User{&userMe}, results)
}
