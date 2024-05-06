package sqlserver

import (
	"context"
	"fmt"
	"github.com/jasonlabz/dbutil/dbx"
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

func TestOP(t *testing.T) {
	ctx := context.Background()
	operator := NewSqlserverOperator()
	err := operator.Open(&dbx.Config{
		DBName: "test",
		DSN:    "user id=SA;password=@HALOjeff02;server=192.168.3.30;port=1433;database=master;encrypt=disable",
		DBType: dbx.DBTypeSqlserver,
	})
	if err != nil {
		panic(err)
	}
	columnsUnderTables, err := operator.GetColumnsUnderTables(ctx, "test", "dbo", []string{"test"})
	if err != nil {
		panic(err)
	}
	println(columnsUnderTables)
}
