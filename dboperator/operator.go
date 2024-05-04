package dboperator

import (
	"context"
	"math"

	"github.com/jasonlabz/dbutil/dbx"
)

// IConnector 数据库连接器接口
type IConnector interface {
	Open(config *dbx.Config) error
	GetDB(name string) (*dbx.DBWrapper, error)
	Ping(dbName string) error
	Close(dbName string) error
}

// IDataExplorer 数据探查
type IDataExplorer interface {
	// GetTablesUnderDB 获取该库下所有逻辑库及表名
	GetTablesUnderDB(ctx context.Context, dbName string) (dbTableMap map[string]*LogicDBInfo, err error)
	// GetTablesUnderSchema 获取模式下表名
	GetTablesUnderSchema(ctx context.Context, dbName string, schemas []string) (dbTableMap map[string]*LogicDBInfo, err error)
	// GetColumns 获取指定库所有逻辑库及表下字段列表
	GetColumns(ctx context.Context, dbName string) (dbTableColMap map[string]map[string]*TableColInfo, err error)
	// GetColumnsUnderTables 获取指定库表下字段列表
	GetColumnsUnderTables(ctx context.Context, dbName, logicDBName string, tableNames []string) (tableColMap map[string]*TableColInfo, err error)
	// CreateSchema 创建逻辑库
	CreateSchema(ctx context.Context, dbName, schemaName, commentInfo string) (err error)
	// GetTablePrimeKeys 查询主键
	GetTablePrimeKeys(ctx context.Context, dbName string, schemaName string, tables []string) (primeKeyInfo map[string][]string, err error)
	// GetTableUniqueKeys 查询唯一键
	GetTableUniqueKeys(ctx context.Context, dbName string, schemaName string, tables []string) (uniqueKeyInfo map[string]map[string][]string, err error)
	// ExecuteDDL 执行DDL
	ExecuteDDL(ctx context.Context, dbName, schemaName string, primaryKeysMap map[string][]string, uniqueKeysMap map[string]map[string][]string, tableFieldsMap map[string][]*Field) (ddlSQL string, err error)
	// GetDataBySQL 执行自定义
	GetDataBySQL(ctx context.Context, dbName, sqlStatement string) (rows []map[string]interface{}, err error)
	// GetTableData 执行查询表数据, pageInfo为nil时不分页
	GetTableData(ctx context.Context, dbName, schemaName, tableName string, pageInfo *Pagination) (rows []map[string]interface{}, err error)
}

type IOperator interface {
	IConnector
	IDataExplorer
	ITransfer
}

type GormDBTable struct {
	TableSchema string `db:"table_schema"`
	TableName   string `db:"table_name"`
	Comments    string `db:"comments"`
}

type TablePrimeKey struct {
	SchemaName string `db:"schema_name"`
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
	IndexName  string `db:"constraint_name"`
}

type GormTableColumn struct {
	TableSchema     string `db:"table_schema"`
	TableName       string `db:"table_name"`
	ColumnName      string `db:"column_name"`
	Comments        string `db:"comments"`
	DataType        string `db:"data_type"`
	IsNullable      bool   `db:"is_nullable"`      // 可否为null
	OrdinalPosition int    `db:"ordinal_position"` // 字段序号
}

type LogicDBInfo struct {
	SchemaName    string
	TableInfoList []*TableInfo
}

type TableInfo struct {
	TableName string // 列名
	Comment   string // 注释
}

type TableColInfo struct {
	TableName      string
	ColumnInfoList []*ColumnInfo // 列
}

type ColumnInfo struct {
	ColumnName      string // 列名
	Comment         string // 注释
	DataType        string // 数据类型
	IsNullable      bool   // 可否为null
	OrdinalPosition int    // 字段序号
}

// Pagination 分页结构体（该分页只适合数据量很少的情况）
type Pagination struct {
	Page      int64 `json:"page"`       // 当前页
	PageSize  int64 `json:"page_size"`  // 每页多少条记录
	PageCount int64 `json:"page_count"` // 一共多少页
	Total     int64 `json:"total"`      // 一共多少条记录
}

func (p *Pagination) SetPageCount() {
	p.PageCount = int64(math.Ceil(float64(p.Total) / float64(p.PageSize)))
	return
}

func (p *Pagination) GetOffset() (offset int64) {
	offset = (p.Page - 1) * p.PageSize
	return
}
