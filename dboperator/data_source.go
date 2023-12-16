package dboperator

import (
	"context"
	"errors"

	"github.com/jasonlabz/dbutil/dboperator/mysql"
	"github.com/jasonlabz/dbutil/dboperator/oracle"
	"github.com/jasonlabz/dbutil/dboperator/postgresql"
	"github.com/jasonlabz/dbutil/dboperator/sqlserver"
	"github.com/jasonlabz/dbutil/gormx"
)

var dsMap = make(map[gormx.DBType]*DS)

type DS struct {
	Operator IOperator
}

// Open open database by config
func (ds *DS) Open(config *gormx.Config) error {
	return ds.Operator.Open(config)
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary
func (ds *DS) Ping(dbName string) error {
	return ds.Operator.Ping(dbName)
}

// Close database by name
func (ds *DS) Close(dbName string) error {
	return ds.Operator.Close(dbName)
}

// GetTablesUnderDB 获取该库下所有逻辑库及表名
func (ds *DS) GetTablesUnderDB(ctx context.Context, dbName string) (dbTableMap map[string]*LogicDBInfo, err error) {
	return ds.Operator.GetTablesUnderDB(ctx, dbName)
}

// GetColumns 获取指定库所有逻辑库及表下字段列表
func (ds *DS) GetColumns(ctx context.Context, dbName string) (dbTableColMap map[string]map[string]*TableColInfo, err error) {
	return ds.Operator.GetColumns(ctx, dbName)
}

// GetColumnsUnderTable 获取指定库表下字段列表
func (ds *DS) GetColumnsUnderTable(ctx context.Context, dbName, logicDBName string, tableNames []string) (tableColMap map[string]*TableColInfo, err error) {
	return ds.Operator.GetColumnsUnderTables(ctx, dbName, logicDBName, tableNames)
}

// CreateSchema 创建逻辑库
func (ds *DS) CreateSchema(ctx context.Context, dbName, schemaName, commentInfo string) (err error) {
	return ds.Operator.CreateSchema(ctx, dbName, schemaName, commentInfo)
}

// ExecuteDDL 执行DDL
func (ds *DS) ExecuteDDL(ctx context.Context, dbName, logicDBName, tableName, ddlStatement string) (err error) {
	return ds.Operator.ExecuteDDL(ctx, dbName, ddlStatement)
}

// GetDataBySQL 执行自定义
func (ds *DS) GetDataBySQL(ctx context.Context, dbName, sqlStatement string) (rows []map[string]interface{}, err error) {
	return ds.Operator.GetDataBySQL(ctx, dbName, sqlStatement)
}

// GetTableData 执行查询表数据, pageInfo为nil时不分页
func (ds *DS) GetTableData(ctx context.Context, dbName, schemaName, tableName string, pageInfo *Pagination) (rows []map[string]interface{}, err error) {
	return ds.Operator.GetTableData(ctx, dbName, schemaName, tableName, pageInfo)
}

func GetDS(dataSourceType gormx.DBType) (ds *DS, err error) {
	var ok bool
	ds, ok = dsMap[dataSourceType]
	if !ok {
		err = errors.New("unsupported db_type")
		return
	}
	return
}

func init() {
	// oracle
	dsMap[gormx.DBTypeOracle] = &DS{
		Operator: oracle.NewOracleOperator(),
	}
	// postgresql
	dsMap[gormx.DBTypePostgres] = &DS{
		Operator: postgresql.NewPGOperator(),
	}
	// mysql
	dsMap[gormx.DBTypeMySQL] = &DS{
		Operator: mysql.NewMySQLOperator(),
	}

	// sqlserver
	dsMap[gormx.DBTypeSqlserver] = &DS{
		Operator: sqlserver.NewSqlserverOperator(),
	}
}
