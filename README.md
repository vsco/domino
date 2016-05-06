# domino
Typesafe DynamoDB query DSL for Go


This is an easy to use wrapper DSL for the aws dynamodb GO api.


```

config := s3.GetAwsConfig("123", "123").WithEndpoint("http://127.0.0.1:8080")
sess := session.New(config)
dynamo := dynamodb.New(sess)

table := DynamoDBTable {
  Name:"Users"
  PartitionKeyName:"email"
  RangeKeyName:&"password"
}

q := table.GetItem(Key{"naveen@email.com", "password"}).SetConsistentRead(true).Build()  //This is type GetItemInput
r, err := dynamo.GetItem(q)

```
