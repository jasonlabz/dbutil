package oracle

import (
	"fmt"
	"testing"
)

func TestDataType(t *testing.T) {
	var dataType = "numeric(3,      10)"
	operator := NewOracleOperator()
	field := operator.Trans2CommonField(dataType)
	fmt.Println(*field)
	trans2DataType := operator.Trans2DataType(field)
	fmt.Println(trans2DataType)
}
