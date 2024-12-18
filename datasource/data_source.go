package datasource

import (
	"context"
	"errors"
	"fmt"

	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/dboperator/dm"
	"github.com/jasonlabz/dbutil/dboperator/mysql"
	"github.com/jasonlabz/dbutil/dboperator/oracle"
	"github.com/jasonlabz/dbutil/dboperator/postgresql"
	"github.com/jasonlabz/dbutil/dboperator/sqlite"
	"github.com/jasonlabz/dbutil/dboperator/sqlserver"
	"github.com/jasonlabz/dbutil/dbx"
)

var dsMap = make(map[dbx.DBType]*DS)

type DS struct {
	Operator dboperator.IOperator
}

func (ds *DS) Trans2CommonField(dataType string) *dboperator.Field {
	return ds.Operator.Trans2CommonField(dataType)
}

func (ds *DS) Trans2DataType(field *dboperator.Field) string {
	return ds.Operator.Trans2DataType(field)
}

// Open open database by config
func (ds *DS) Open(config *dbx.Config) error {
	return ds.Operator.Open(config)
}

// GetDB GetDB instance by name
func (ds *DS) GetDB(name string) (*dbx.DBWrapper, error) {
	return ds.Operator.GetDB(name)
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary
func (ds *DS) Ping(dbName string) error {
	return ds.Operator.Ping(dbName)
}

// Close database by name
func (ds *DS) Close(dbName string) error {
	return ds.Operator.Close(dbName)
}

// GetTablesUnderSchema 获取该逻辑库下所有表名
func (ds *DS) GetTablesUnderSchema(ctx context.Context, dbName string, schemas []string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
	return ds.Operator.GetTablesUnderSchema(ctx, dbName, schemas)
}

// GetTablesUnderDB 获取该库下所有逻辑库及表名
func (ds *DS) GetTablesUnderDB(ctx context.Context, dbName string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
	return ds.Operator.GetTablesUnderDB(ctx, dbName)
}

// GetColumns 获取指定库所有逻辑库及表下字段列表
func (ds *DS) GetColumns(ctx context.Context, dbName string) (dbTableColMap map[string]map[string]*dboperator.TableColInfo, err error) {
	return ds.Operator.GetColumns(ctx, dbName)
}

// GetColumnsUnderTable 获取指定库表下字段列表
func (ds *DS) GetColumnsUnderTable(ctx context.Context, dbName, logicDBName string, tableNames []string) (tableColMap map[string]*dboperator.TableColInfo, err error) {
	return ds.Operator.GetColumnsUnderTables(ctx, dbName, logicDBName, tableNames)
}

// CreateSchema 创建逻辑库
func (ds *DS) CreateSchema(ctx context.Context, dbName, schemaName, commentInfo string) (err error) {
	return ds.Operator.CreateSchema(ctx, dbName, schemaName, commentInfo)
}

// GetTablePrimeKeys 查询主键
func (ds *DS) GetTablePrimeKeys(ctx context.Context, dbName string, schemaName string, tables []string) (primeKeyInfo map[string][]string, err error) {
	return ds.Operator.GetTablePrimeKeys(ctx, dbName, schemaName, tables)
}

// GetTableUniqueKeys 查询唯一键
func (ds *DS) GetTableUniqueKeys(ctx context.Context, dbName string, schemaName string, tables []string) (uniqueKeyInfo map[string]map[string][]string, err error) {
	return ds.Operator.GetTableUniqueKeys(ctx, dbName, schemaName, tables)
}

// ExecuteDDL 执行DDL
func (ds *DS) ExecuteDDL(ctx context.Context, dbName, schemaName string, primaryKeysMap map[string][]string, uniqueKeysMap map[string]map[string][]string, tableFieldsMap map[string][]*dboperator.Field) (ddlSQL string, err error) {
	return ds.Operator.ExecuteDDL(ctx, dbName, schemaName, primaryKeysMap, uniqueKeysMap, tableFieldsMap)
}

// GetDataBySQL 执行自定义
func (ds *DS) GetDataBySQL(ctx context.Context, dbName, sqlStatement string) (rows []map[string]interface{}, err error) {
	return ds.Operator.GetDataBySQL(ctx, dbName, sqlStatement)
}

// GetTableData 执行查询表数据, pageInfo为nil时不分页
func (ds *DS) GetTableData(ctx context.Context, dbName, schemaName, tableName string, pageInfo *dboperator.Pagination) (rows []map[string]interface{}, err error) {
	return ds.Operator.GetTableData(ctx, dbName, schemaName, tableName, pageInfo)
}

func LoadDS(dataSourceType dbx.DBType) (ds *DS, err error) {
	var ok bool
	ds, ok = dsMap[dataSourceType]
	if !ok {
		err = errors.New("unsupported db_type")
		return
	}
	return
}

func RegisterDS(dataSourceType dbx.DBType, operator dboperator.IOperator) error {
	var ok bool
	_, ok = dsMap[dataSourceType]
	if ok {
		return fmt.Errorf("db_type %s is already registered")
	}
	dsMap[dataSourceType] = &DS{
		Operator: operator,
	}
	return nil
}

func init() {
	err := RegisterDS(dbx.DBTypeMySQL, mysql.NewMySQLOperator())
	if err != nil {
		panic(err)
	}

	err = RegisterDS(dbx.DBTypeOracle, oracle.NewOracleOperator())
	if err != nil {
		panic(err)
	}

	err = RegisterDS(dbx.DBTypePostgres, postgresql.NewPGOperator())
	if err != nil {
		panic(err)
	}

	err = RegisterDS(dbx.DBTypeSqlserver, sqlserver.NewSqlserverOperator())
	if err != nil {
		panic(err)
	}

	err = RegisterDS(dbx.DBTypeDM, dm.NewDMOperator())
	if err != nil {
		panic(err)
	}

	err = RegisterDS(dbx.DBTypeSQLite, sqlite.NewSQLiteOperator())
	if err != nil {
		panic(err)
	}
}
