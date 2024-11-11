package db2

import (
	"database/sql"
	"database/sql/driver"

	"github.com/ibmdb/go_ibm_db"
	"github.com/jasonlabz/dagine/etl/storage/database"
	"github.com/pingcap/errors"
)

// Table db2表
type Table struct {
	*database.BaseTable
	database.BaseConfigSetter
}

// NewTable 创建db2表，注意此时BaseTable中的schema参数为空，instance为数据库名，而name是表明
func NewTable(b *database.BaseTable) *Table {
	return &Table{
		BaseTable: b,
	}
}

// Quoted 表引用全名
func (t *Table) Quoted() string {
	return Quoted(t.Schema()) + "." + Quoted(t.Name())
}

func (t *Table) String() string {
	return t.Quoted()
}

// AddField 新增列
func (t *Table) AddField(baseField *database.BaseField) {
	f := NewField(baseField)
	f.SetConfig(t.Config())
	t.AppendField(f)
}

// ExecParam 获取执行参数
func (t *Table) ExecParam(mode string, txOpts *sql.TxOptions) (database.Parameter, bool) {
	return nil, false
}

// ShouldRetry 重试
func (t *Table) ShouldRetry(err error) bool {
	return errors.Cause(err) == driver.ErrBadConn
}

// ShouldOneByOne 单个重试
func (t *Table) ShouldOneByOne(err error) bool {
	_, ok := errors.Cause(err).(*go_ibm_db.Error)
	return ok
}
