package domino

import (
	"fmt"
	// "github.	com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"strconv"
	"testing"
	"time"
)

type MyTable struct {
	DynamoTable
	emailField    dynamoFieldString
	passwordField dynamoFieldString

	registrationDate dynamoFieldNumeric
	loginCount       dynamoFieldNumeric
	lastLoginDate    dynamoFieldNumeric
	vists            dynamoFieldNumericSet
	preferences      dynamoFieldMap
	nameField        dynamoFieldString
	lastNameField    dynamoFieldString

	registrationDateIndex LocalSecondaryIndex
	nameGlobalIndex       GlobalSecondaryIndex
}

type User struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func NewMyTable() MyTable {
	pk := DynamoFieldString("email")
	rk := DynamoFieldString("password")
	firstName := DynamoFieldString("firstName")
	lastName := DynamoFieldString("lastName")
	reg := DynamoFieldNumeric("registrationDate")
	return MyTable{
		DynamoTable{
			Name:         "mytable",
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

func TestGetItem(b *testing.T) {
	table := NewMyTable()

	q := table.GetItem(KeyValue{"test", "password"}).Build()
	fmt.Println(q)
}

func TestBatchGetItem(b *testing.T) {
	table := NewMyTable()

	q := table.
		BatchGetItem(
			KeyValue{"naveen@email.com", "password"},
			KeyValue{"joe@email.com", "password"},
		).
		SetConsistentRead(true).Build()
	fmt.Println(q)
}

func TestUpdateItem(b *testing.T) {
	table := NewMyTable()

	q := table.
		UpdateItem(KeyValue{"naveen@email.com", "password"}).
		SetUpdateExpression(
			table.loginCount.Increment(1),
			table.lastLoginDate.SetField(time.Now().UnixNano(), false),
			table.registrationDate.SetField(time.Now().UnixNano(), true),
			table.vists.RemoveElemIndex(0),
			table.preferences.RemoveKey("update_email"),
		).Build()

	fmt.Println(q)
}

func TestQuery(b *testing.T) {
	table := NewMyTable()
	p := table.lastNameField.Equals("Gattu")
	q := table.
		Query(
			table.nameField.Equals("naveen"),
			&p,
		).
		SetLimit(100).
		SetScanForward(true).
		SetLocalIndex(table.registrationDateIndex).
		Build()
	fmt.Println(q)
}

func TestPutItem(b *testing.T) {
	table := NewMyTable()
	item := User{Email: "joe@email.com", Password: "password"}

	q := table.
		PutItem(item).
		ReturnOld().
		SetConditionExpression(
			table.vists.Size(lte, 25)).
		Build()
	fmt.Printf("%++v", q)
}
func TestExpressions(b *testing.T) {
	table := NewMyTable()

	q := Or(
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
	s, m, _ := q.construct(0)
	fmt.Println(s)
	fmt.Println(m)

}
