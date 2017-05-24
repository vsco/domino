package domino

import (
	// "fmt"

	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

const localDynamoHost = "http://127.0.0.1:4569"

type UserTable struct {
	DynamoTable
	emailField    String
	passwordField String

	registrationDate Numeric
	loginCount       Numeric
	lastLoginDate    Numeric
	visits           NumericSet
	preferences      Map
	nameField        String
	lastNameField    String

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
	pk := StringField("email")
	rk := StringField("password")
	firstName := StringField("firstName")
	lastName := StringField("lastName")
	reg := NumericField("registrationDate")
	nameGlobalIndex := GlobalSecondaryIndex{
		Name:             "name-index",
		PartitionKey:     firstName,
		RangeKey:         lastName,
		ProjectionType:   ProjectionTypeINCLUDE,
		NonKeyAttributes: []DynamoFieldIFace{lastName, reg},
	}

	registrationDateIndex := LocalSecondaryIndex{
		Name:         "registrationDate-index",
		PartitionKey: pk,
		SortKey:      reg,
	}

	return UserTable{
		DynamoTable{
			Name:         "dev-ore-feed",
			PartitionKey: pk,
			RangeKey:     rk,
			GlobalSecondaryIndexes: []GlobalSecondaryIndex{
				nameGlobalIndex,
			},
			LocalSecondaryIndexes: []LocalSecondaryIndex{
				registrationDateIndex,
			},
		},
		pk,  //email
		rk,  //password
		reg, //registration
		NumericField("loginCount"),
		NumericField("lastLoginDate"),
		NumericSetField("visits"),
		MapField("preferences"),
		firstName,
		lastName,
		registrationDateIndex,
		nameGlobalIndex,
	}
}

func NewDB() DynamoDBIFace {
	region := "us-west-2"
	creds := credentials.NewStaticCredentials("123", "123", "")
	config := aws.
		NewConfig().
		WithRegion(region).
		WithCredentials(creds).
		WithEndpoint(localDynamoHost).
		WithHTTPClient(http.DefaultClient)
	sess := session.New(config)

	return dynamodb.New(sess)
}

func TestGetItem(t *testing.T) {

	ctx := context.Background()

	table := NewUserTable()

	db := NewDB()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	item := User{Email: "naveen@email.com", Password: "password"}
	err = table.PutItem(item).ExecuteWith(ctx, db)
	assert.Nil(t, err)

	r, err := table.GetItem(KeyValue{"naveen@email.com", "password"}).ExecuteWith(ctx, db, &User{})
	assert.Nil(t, err)
	assert.Equal(t, User{Email: "naveen@email.com", Password: "password"}, *r.(*User))
}
func TestGetItemEmpty(t *testing.T) {

	table := NewUserTable()

	db := NewDB()

	ctx := context.Background()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	user, err := table.GetItem(KeyValue{"naveen@email.com", "password"}).ExecuteWith(ctx, db, &User{})
	assert.Nil(t, err)
	assert.Nil(t, user)
}

func TestBatchPutItem(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()
	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

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
	err = q.ExecuteWith(ctx, db, func() interface{} {
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
	g.ExecuteWith(ctx, db, func() interface{} {
		user := User{}
		users = append(users, &user)
		return &user
	})

	assert.NotEmpty(t, users)
}

func TestBatchGetItem(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()
	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	u := &User{Email: "bob@email.com", Password: "password"}
	items := []interface{}{u}
	kvs := []KeyValue{}
	for i := 0; i < 200; i++ {
		items = append(items, &User{Email: "bob@email.com", Password: "password" + strconv.Itoa(i)})
		kvs = append(kvs, KeyValue{"bob@email.com", "password" + strconv.Itoa(i)})
	}

	ui := []*User{}
	w := table.BatchWriteItem().PutItems(items...)

	err = w.ExecuteWith(ctx, db, func() interface{} {
		u := User{}
		ui = append(ui, &u)
		return &u
	})

	assert.Nil(t, err)
	assert.Empty(t, ui)

	g := table.BatchGetItem(kvs...)

	users := []*User{}
	err = g.ExecuteWith(ctx, db, func() interface{} {
		user := User{}
		users = append(users, &user)
		return &user
	})

	assert.Nil(t, err)
	assert.Equal(t, len(users), 200)
}

func TestUpdateItem(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	item := User{Email: "naveen@email.com", Password: "password", Visits: []int64{time.Now().UnixNano()}}
	q := table.PutItem(item)
	err = q.ExecuteWith(ctx, db)

	assert.Nil(t, err)

	now := time.Now().UnixNano()

	u := table.
		UpdateItem(KeyValue{"naveen@email.com", "password"}).
		SetUpdateExpression(
			table.loginCount.Increment(1),
			table.lastLoginDate.SetField(now, false),
			table.registrationDate.SetField(now, true),
			table.visits.Append(now),
			table.preferences.RemoveKey("update_email"),
		)

	err = u.ExecuteWith(ctx, db)
	assert.Nil(t, err)
	g := table.GetItem(KeyValue{"naveen@email.com", "password"})

	user, err := g.ExecuteWith(ctx, db, &User{})

	assert.NotNil(t, user)
	du := user.(*User)
	assert.Equal(t, du.LoginCount, 1)
	assert.Equal(t, du.RegDate, int64(0))
	assert.Contains(t, du.Visits, now)
	assert.NotContains(t, du.Preferences, "update_email")
}

func TestRemoveAttribute(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	q := table.PutItem(User{Email: "brendanr@email.com", Password: "password", LoginCount: 5})
	err = q.ExecuteWith(ctx, db)
	assert.Nil(t, err)

	// remove
	u := table.
		UpdateItem(KeyValue{"brendanr@email.com", "password"}).
		SetUpdateExpression(
			table.registrationDate.SetField(time.Now().UnixNano(), true),
			table.loginCount.RemoveField(),
		)
	err = u.ExecuteWith(ctx, db)
	assert.Nil(t, err)

	g := table.GetItem(KeyValue{"brendanr@email.com", "password"})
	user, err := g.ExecuteWith(ctx, db, &User{})
	assert.NotNil(t, user)

	du := user.(*User)
	assert.Equal(t, 0, du.LoginCount)
}

func TestPutItem(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	item := User{Email: "joe@email.com", Password: "password"}
	q := table.PutItem(item)
	err = q.ExecuteWith(ctx, db)

	now := time.Now().UnixNano()
	v := table.
		UpdateItem(
			KeyValue{"joe@email.com", "password"},
		).
		SetUpdateExpression(
			table.loginCount.Increment(1),
			table.registrationDate.SetField(now, false),
		)
	err = v.ExecuteWith(ctx, db)

	assert.Nil(t, err)

	g := table.GetItem(KeyValue{"joe@email.com", "password"})

	user, err := g.ExecuteWith(ctx, db, &User{})

	assert.NotNil(t, user)
	du := user.(*User)

	assert.Equal(t, du.LoginCount, 1)
	assert.Equal(t, du.RegDate, now)
}

func TestExpressions(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	expr := Or(
		table.registrationDate.BeginsWith("t"),
		table.lastNameField.Contains("25"),
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

	expectedFilter := "begins_with(registrationDate,:t_1) OR contains(lastName,:25_2) OR (NOT contains(registrationDate,:t_3)) OR (size(registrationDate) <=:25_4 AND size(firstName) >=:25_5) OR registrationDate = :test_6 OR registrationDate <= :test_7 OR (registrationDate between :0_8 and :1_9) OR (registrationDate in (:0_10,:1_11))"
	assert.Equal(t, expectedFilter, *q.Build().FilterExpression)

	channel, errChan := q.StreamWith(ctx, db, User{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-channel:
			case err = <-errChan:
				return
			}
		}
	}()

	wg.Wait()

	assert.Nil(t, err)
}

func TestDynamoQuery(t *testing.T) {

	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	me := &User{Email: "naveen@email.com", Password: "password"}
	items := []interface{}{me}
	for i := 0; i < 1000; i++ {
		e := "naveen@email.com"
		items = append(items, &User{Email: e, Password: "password" + strconv.Itoa(i)})
	}

	ui := []*User{}
	w := table.BatchWriteItem().PutItems(items...)

	err = w.ExecuteWith(ctx, db, func() interface{} {
		u := User{}
		ui = append(ui, &u)
		return &u
	})

	assert.Nil(t, err)

	assert.Empty(t, ui)

	limit := 100
	p := table.passwordField.BeginsWith("password")
	q := table.
		Query(
			table.emailField.Equals("naveen@email.com"),
			&p,
		).
		SetLimit(limit).
		SetPageSize(10).
		SetScanForward(true)

	items, err = q.ExecuteWith(ctx, db, &User{})

	assert.Nil(t, err)
	assert.Equal(t, limit, len(items))
}

func TestDynamoStreamQuery(t *testing.T) {

	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	me := &User{Email: "naveen@email.com", Password: "password"}
	items := []interface{}{me}
	for i := 0; i < 1000; i++ {
		e := "naveen@email.com"
		items = append(items, &User{Email: e, Password: "password" + strconv.Itoa(i)})
	}

	ui := []*User{}
	w := table.BatchWriteItem().PutItems(items...)

	err = w.ExecuteWith(ctx, db, func() interface{} {
		u := User{}
		ui = append(ui, &u)
		return &u
	})

	assert.Nil(t, err)
	assert.Empty(t, ui)

	set := false
	f := func(c *dynamodb.ConsumedCapacity) {
		set = true
	}

	limit := 10
	p := table.passwordField.BeginsWith("password")
	q := table.
		Query(
			table.emailField.Equals("naveen@email.com"),
			&p,
		).SetLimit(limit).WithConsumedCapacityHandler(f).SetScanForward(true)

	users := []User{}
	channel := make(chan *User)

	_ = q.StreamWithChannel(ctx, db, channel)

	for u := range channel {
		users = append(users, *u)
	}
	assert.True(t, set)
	assert.Nil(t, err)
	assert.Equal(t, limit, len(users))
}

func TestDynamoQueryError(t *testing.T) {
	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	me := &User{Email: "naveen@email.com", Password: "password"}
	items := []interface{}{me}
	for i := 0; i < 1000; i++ {
		e := "naveen@email.com"
		items = append(items, &User{Email: e, Password: "password" + strconv.Itoa(i)})
	}

	_ = table.BatchWriteItem().PutItems(items...).ExecuteWith(ctx, db, func() interface{} { return &User{} })

	channel, errChan := table.
		Query(
			table.registrationDate.Equals("naveen@email.com"),
			nil,
		).
		SetScanForward(true).
		StreamWith(ctx, db, &User{})

	users := []User{}
SELECT:
	for {
		select {
		case u := <-channel:
			if u != nil {
				users = append(users, *u.(*User))
			} else {
				break SELECT
			}
		case err = <-errChan:
			break SELECT
		}
	}

	assert.NotNil(t, err)

}

func TestDynamoScan(t *testing.T) {

	table := NewUserTable()
	db := NewDB()
	ctx := context.Background()

	err := table.CreateTable().ExecuteWith(ctx, db)
	defer table.DeleteTable().ExecuteWith(ctx, db)

	assert.Nil(t, err)

	me := &User{Email: "naveen@email.com", Password: "password"}
	items := []interface{}{me}
	for i := 0; i < 1000; i++ {
		e := "naveen@email.com"
		items = append(items, &User{Email: e, Password: "password" + strconv.Itoa(i)})
	}

	ui := []*User{}
	w := table.BatchWriteItem().PutItems(items...)

	err = w.ExecuteWith(ctx, db, func() interface{} {
		u := User{}
		ui = append(ui, &u)
		return &u
	})

	assert.Nil(t, err)

	assert.Empty(t, ui)

	limit := 1000
	users := []User{}

	channel, errChan := table.Scan().SetLimit(limit).ExecuteWith(ctx, db, &User{})

SELECT:
	for {
		select {
		case u := <-channel:
			if u != nil {
				users = append(users, *u.(*User))
			} else {
				break SELECT
			}
		case err = <-errChan:
			break SELECT
		}
	}

	assert.Nil(t, err)
	assert.Equal(t, limit, len(users))
}
