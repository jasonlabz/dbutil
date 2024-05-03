package sqlserver

import (
	"fmt"
	"testing"
)

func TestDataType(t *testing.T) {
	var dataType = "bit(8)"
	operator := NewSqlserverOperator()
	field := operator.Trans2CommonField(dataType)
	fmt.Println(*field)
	trans2DataType := operator.Trans2DataType(field)
	fmt.Println(trans2DataType)
}
