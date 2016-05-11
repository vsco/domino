# domino
Typesafe DynamoDB query DSL for Go


This is an easy to use wrapper DSL for the aws dynamodb GO api.


```

config := s3.GetAwsConfig("123", "123").WithEndpoint("http://127.0.0.1:8080")
sess := session.New(config)
dynamo := dynamodb.New(sess)

//Define your table schema statically
table := domino.DynamoTable{
		Name:             "Users",
		PartitionKeyName: "email",
		RangeKeyName:     "password",
	}
	testField := domino.DynamoField{
		Table: table,
		Name:  "test",
		Type:  S,
	}
	thatField := domino.DynamoField{
		Table: table,
		Name:  "that",
		Type:  N,
	}
	otherField := domino.DynamoField{
		Table: table,
		Name:  "other",
		Type:  N,
	}

	q := domino.Or(
		testField.BeginsWith("t"),
		otherField.Contains(strconv.Itoa(25)),
		Not(testField.Contains("t")),
		And(
			testField.Size(lte, 25),
			thatField.Size(gte, 25),
		),
		testField.Equals("test"),
		testField.LessThanOrEq("test"),
		testField.Between("0", "1"),
		testField.In("0", "1"),
	)
r, err := dynamo.GetItem(q)

```
