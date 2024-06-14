package sqlite

import (
	"fmt"
	"testing"
)

func TestDataType(t *testing.T) {
	var dataType = "number(3,  10)"
	operator := NewSQLiteOperator()
	field := operator.Trans2CommonField(dataType)
	fmt.Println(*field)
	trans2DataType := operator.Trans2DataType(field)
	fmt.Println(trans2DataType)
}
