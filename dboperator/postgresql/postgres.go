package postgresql

import (
	"context"
	"errors"
	"fmt"

	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/gormx"
)

func NewPGOperator() dboperator.IOperator {
	return &PGOperator{}
}

type PGOperator struct{}

func (P PGOperator) Open(config *gormx.Config) error {
	return gormx.InitConfig(config)
}

func (P PGOperator) Ping(dbName string) error {
	return gormx.Ping(dbName)
}

func (P PGOperator) Close(dbName string) error {
	return gormx.Close(dbName)
}

func (P PGOperator) GetDataBySQL(ctx context.Context, dbName, sqlStatement string) (rows []map[string]interface{}, err error) {
	rows = make([]map[string]interface{}, 0)
	db, err := gormx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.WithContext(ctx).
		Raw(sqlStatement).
		Find(&rows).Error
	return
}

func (P PGOperator) GetTableData(ctx context.Context, dbName, schemaName, tableName string, pageInfo *dboperator.Pagination) (rows []map[string]interface{}, err error) {
	rows = make([]map[string]interface{}, 0)
	db, err := gormx.GetDB(dbName)
	if err != nil {
		return
	}
	queryTable := fmt.Sprintf("\"%s\"", tableName)
	if schemaName != "" {
		queryTable = fmt.Sprintf("\"%s\".\"%s\"", schemaName, tableName)
	}
	var count int64
	err = db.WithContext(ctx).
		Table(queryTable).
		Count(&count).
		Offset(int(pageInfo.GetOffset())).
		Limit(int(pageInfo.PageSize)).
		Find(&rows).Error
	pageInfo.Total = count
	pageInfo.SetPageCount()
	return
}

func (P PGOperator) GetTablesUnderDB(ctx context.Context, dbName string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
	dbTableMap = make(map[string]*dboperator.LogicDBInfo)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	gormDBTables := make([]*dboperator.GormDBTable, 0)
	db, err := gormx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.WithContext(ctx).
		Raw("SELECT tb.schemaname as table_schema, " +
			"tb.tablename as table_name, " +
			"d.description as comments " +
			"FROM pg_tables tb " +
			"JOIN pg_class c ON c.relname = tb.tablename " +
			"LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = '0' " +
			"WHERE schemaname <> 'information_schema' " +
			"AND tablename NOT LIKE 'pg%' " +
			"AND tablename NOT LIKE 'gp%' " +
			"AND tablename NOT LIKE 'sql_%' " +
			"ORDER BY tb.schemaname, tb.tablename").
		Find(&gormDBTables).Error
	if len(gormDBTables) == 0 {
		return
	}
	for _, row := range gormDBTables {
		if logicDBInfo, ok := dbTableMap[row.TableSchema]; !ok {
			dbTableMap[row.TableSchema] = &dboperator.LogicDBInfo{
				SchemaName: row.TableSchema,
				TableInfoList: []*dboperator.TableInfo{{
					TableName: row.TableName,
					Comment:   row.Comments,
				}},
			}
		} else {
			logicDBInfo.TableInfoList = append(logicDBInfo.TableInfoList,
				&dboperator.TableInfo{
					TableName: row.TableName,
					Comment:   row.Comments,
				})
		}
	}
	return
}

func (P PGOperator) GetColumns(ctx context.Context, dbName string) (dbTableColMap map[string]map[string]*dboperator.TableColInfo, err error) {
	dbTableColMap = make(map[string]map[string]*dboperator.TableColInfo, 0)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	gormTableColumns := make([]*dboperator.GormTableColumn, 0)
	db, err := gormx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.WithContext(ctx).
		Raw("select " +
			"ic.table_schema table_schema, " +
			"ic.table_name table_name, " +
			"ic.column_name as column_name, " +
			"ic.udt_name as data_type, " +
			"d.description as comments " +
			"from " +
			"information_schema.columns ic " +
			"JOIN pg_class c ON c.relname = ic.table_name " +
			"LEFT JOIN pg_description d " +
			"ON d.objoid = c.oid AND d.objsubid = ic.ordinal_position " +
			"where ic.table_name NOT LIKE 'pg%' " +
			"AND ic.table_name NOT LIKE 'gp%' " +
			"AND ic.table_name NOT LIKE 'sql_%' " +
			"AND ic.table_schema <> 'information_schema' " +
			"ORDER BY ic.table_name, ic.ordinal_position").
		Find(&gormTableColumns).Error
	if err != nil {
		return
	}
	if len(gormTableColumns) == 0 {
		return
	}

	for _, row := range gormTableColumns {
		if dbTableColInfoMap, ok := dbTableColMap[row.TableSchema]; !ok {
			dbTableColMap[row.TableSchema] = map[string]*dboperator.TableColInfo{
				row.TableName: {
					TableName: row.TableName,
					ColumnInfoList: []*dboperator.ColumnInfo{{
						ColumnName: row.ColumnName,
						Comment:    row.Comments,
						DataType:   row.DataType,
					}},
				},
			}
		} else if tableColInfo, ok_ := dbTableColInfoMap[row.TableName]; !ok_ {
			dbTableColInfoMap[row.TableName] = &dboperator.TableColInfo{
				TableName: row.TableName,
				ColumnInfoList: []*dboperator.ColumnInfo{{
					ColumnName: row.ColumnName,
					Comment:    row.Comments,
					DataType:   row.DataType,
				}},
			}
		} else {
			tableColInfo.ColumnInfoList = append(tableColInfo.ColumnInfoList, &dboperator.ColumnInfo{
				ColumnName: row.ColumnName,
				Comment:    row.Comments,
				DataType:   row.DataType,
			})
		}
	}
	return
}

func (P PGOperator) GetColumnsUnderTables(ctx context.Context, dbName, logicDBName string, tableNames []string) (tableColMap map[string]*dboperator.TableColInfo, err error) {
	tableColMap = make(map[string]*dboperator.TableColInfo, 0)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	if len(tableNames) == 0 {
		err = errors.New("empty tableNames")
		return
	}

	gormTableColumns := make([]*dboperator.GormTableColumn, 0)
	db, err := gormx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.WithContext(ctx).
		Raw("select "+
			"ic.table_schema table_schema, "+
			"ic.table_name table_name, "+
			"ic.column_name as column_name, "+
			"ic.udt_name as data_type, "+
			"d.description as comments "+
			"from "+
			"information_schema.columns ic "+
			"JOIN pg_class c ON c.relname = ic.table_name "+
			"LEFT JOIN pg_description d "+
			"ON d.objoid = c.oid AND d.objsubid = ic.ordinal_position "+
			"where "+
			"ic.table_schema = ? "+
			"and ic.table_name in ? "+
			"ORDER BY ic.table_name, ic.ordinal_position", logicDBName, tableNames).
		Find(&gormTableColumns).Error
	if err != nil {
		return
	}
	if len(gormTableColumns) == 0 {
		return
	}

	for _, row := range gormTableColumns {
		if tableColInfo, ok := tableColMap[row.TableName]; !ok {
			tableColMap[row.TableName] = &dboperator.TableColInfo{
				TableName: row.TableName,
				ColumnInfoList: []*dboperator.ColumnInfo{{
					ColumnName: row.ColumnName,
					Comment:    row.Comments,
					DataType:   row.DataType,
				}},
			}
		} else {
			tableColInfo.ColumnInfoList = append(tableColInfo.ColumnInfoList, &dboperator.ColumnInfo{
				ColumnName: row.ColumnName,
				Comment:    row.Comments,
				DataType:   row.DataType,
			})
		}
	}
	return
}

func (P PGOperator) CreateSchema(ctx context.Context, dbName, schemaName, commentInfo string) (err error) {
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	if commentInfo == "" {
		commentInfo = schemaName
	}
	db, err := gormx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.WithContext(ctx).Exec("create schema if not exists " + schemaName).Error
	if err != nil {
		return
	}
	commentStr := fmt.Sprintf("comment on schema %s is '%s'", schemaName, commentInfo)
	err = db.WithContext(ctx).Exec(commentStr).Error
	if err != nil {
		return
	}
	return
}

func (P PGOperator) ExecuteDDL(ctx context.Context, dbName, ddlStatement string) (err error) {
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	db, err := gormx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.WithContext(ctx).Exec(ddlStatement).Error
	if err != nil {
		return
	}
	return
}
