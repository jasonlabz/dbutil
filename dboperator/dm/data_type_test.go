package dm

import (
	"context"
	"fmt"
	"testing"

	"github.com/jasonlabz/dbutil/dbx"
)

func TestDataType(t *testing.T) {
	var dataType = "numeric(3,      10)"
	operator := NewDMOperator()
	field := operator.Trans2CommonField(dataType)
	fmt.Println(*field)
	trans2DataType := operator.Trans2DataType(field)
	fmt.Println(trans2DataType)
}

func TestOP(t *testing.T) {
	ctx := context.Background()
	operator := NewDMOperator()
	err := operator.Open(&dbx.Config{
		DBName: "test",
		DSN:    "lucas/openthedoor@192.168.3.30:1521/XE",
		DBType: dbx.DBTypeOracle,
	})
	if err != nil {
		panic(err)
	}
	columnsUnderTables, err := operator.GetColumnsUnderTables(ctx, "test", "LUCAS", []string{"TEST"})
	if err != nil {
		panic(err)
	}
	println(columnsUnderTables)
}
