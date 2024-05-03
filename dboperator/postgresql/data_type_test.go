package postgresql

import (
	"fmt"
	"testing"
)

func TestDataType(t *testing.T) {
	var dataType = "jsonb"
	operator := NewPGOperator()
	field := operator.Trans2CommonField(dataType)
	fmt.Println(*field)
	trans2DataType := operator.Trans2DataType(field)
	fmt.Println(trans2DataType)
}
