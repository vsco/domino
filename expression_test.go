package domino

import (
	"fmt"
	"strconv"
	"testing"
)

func TestExpressions(b *testing.T) {

	q := Or(
		Path("test").BeginsWith("t"),
		Path("other").Contains(strconv.Itoa(25)),
		Not(Path("this").Contains("t")),
		And(
			Path("this").Size(lte, 25),
			Path("that").Size(gte, 25),
		),
		Path("test").Equals("test"),
		Path("test").LessThanOrEq("test"),
		Path("test").Between("0", "1"),
		Path("test").In("0", "1"),
	)

	fmt.Println(q)
	fmt.Println(q.attributeValues())
}
