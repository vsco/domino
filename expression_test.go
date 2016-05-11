package domino

import (
	"fmt"
	// "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"strconv"
	"testing"
)

type MyTable struct {
	DynamoTable
	emailField    dynamoFieldString
	passwordField emptyDynamoField

	thisField  dynamoFieldNumeric
	thatField  dynamoFieldString
	otherField dynamoFieldString
}

type User struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func NewMyTable() MyTable {
	pk := DynamoFieldString("email")
	rk := EmptyDynamoField()
	return MyTable{
		DynamoTable{
			Name:         "mytable",
			PartitionKey: pk,
			RangeKey:     rk,
		},
		pk,
		rk,
		DynamoFieldNumeric("test"),
		DynamoFieldString("that"),
		DynamoFieldString("other"),
	}
}

func TestGetItem(b *testing.T) {
	table := NewMyTable()

	q := table.GetItem(KeyValue{"test", "password"}).Build()
	fmt.Println(q)
}

func TestBatchGetItem(b *testing.T) {
	table := NewMyTable()

	q := table.BatchGetItem(KeyValue{"test", "password"}).SetConsistentRead(true).Build()
	fmt.Println(q)
}

func TestQuery(b *testing.T) {
	table := NewMyTable()
	p := table.passwordField.Equals("password")
	q := table.Query(table.emailField.Equals("naveen@email.com"), &p).Build()
	fmt.Println(q)
}

func TestPutItem(b *testing.T) {
	table := NewMyTable()
	item := User{Email: "test", Password: "password"}

	q := table.
		PutItem(item).
		ReturnOld().
		SetConditionExpression(
			table.thisField.Size(lte, 25)).
		Build()
	fmt.Printf("%++v", q)
}
func TestExpressions(b *testing.T) {
	table := NewMyTable()

	q := Or(
		table.thisField.BeginsWith("t"),
		table.otherField.Contains(strconv.Itoa(25)),
		Not(table.thisField.Contains("t")),
		And(
			table.thisField.Size(lte, 25),
			table.thatField.Size(gte, 25),
		),
		table.thisField.Equals("test"),
		table.thisField.LessThanOrEq("test"),
		table.thisField.Between("0", "1"),
		table.thisField.In("0", "1"),
	)
	s, m, _ := q.construct(0)
	fmt.Println(s)
	fmt.Println(m)

}
