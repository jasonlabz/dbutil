package postgresql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jasonlabz/dbutil/core/utils"
	"strings"

	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/dbx"
)

func NewPGOperator() dboperator.IOperator {
	return &PGOperator{}
}

type PGOperator struct{}

func (p PGOperator) GetDB(name string) (*dbx.DBWrapper, error) {
	return dbx.GetDB(name)
}

func (p PGOperator) Open(config *dbx.Config) error {
	return dbx.InitConfig(config)
}

func (p PGOperator) Ping(dbName string) error {
	return dbx.Ping(dbName)
}

func (p PGOperator) Close(dbName string) error {
	return dbx.Close(dbName)
}

func (p PGOperator) GetDataBySQL(ctx context.Context, dbName, sqlStatement string) (rows []map[string]interface{}, err error) {
	rows = make([]map[string]interface{}, 0)
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.DB.WithContext(ctx).
		Raw(sqlStatement).
		Find(&rows).Error
	return
}

func (p PGOperator) GetTableData(ctx context.Context, dbName, schemaName, tableName string, pageInfo *dboperator.Pagination) (rows []map[string]interface{}, err error) {
	rows = make([]map[string]interface{}, 0)
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	queryTable := fmt.Sprintf("\"%s\"", tableName)
	if schemaName != "" {
		queryTable = fmt.Sprintf("\"%s\".\"%s\"", schemaName, tableName)
	}
	var count int64
	err = db.DB.WithContext(ctx).
		Table(queryTable).
		Count(&count).
		Offset(int(pageInfo.GetOffset())).
		Limit(int(pageInfo.PageSize)).
		Find(&rows).Error
	pageInfo.Total = count
	pageInfo.SetPageCount()
	return
}

func (p PGOperator) GetTablesUnderSchema(ctx context.Context, dbName string, schemas []string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
	dbTableMap = make(map[string]*dboperator.LogicDBInfo)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	for index, schema := range schemas {
		schemas[index] = "'" + schema + "'"
	}
	gormDBTables := make([]*dboperator.GormDBTable, 0)
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.DB.WithContext(ctx).
		Raw("SELECT distinct tb.schemaname as table_schema, " +
			"tb.tablename as table_name, " +
			"d.description as comments " +
			"FROM pg_tables tb " +
			"JOIN pg_class c ON c.relname = tb.tablename " +
			"LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = '0' " +
			"WHERE schemaname in (" + strings.Join(schemas, ",") + ") " +
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

func (p PGOperator) GetTablesUnderDB(ctx context.Context, dbName string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
	dbTableMap = make(map[string]*dboperator.LogicDBInfo)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	gormDBTables := make([]*dboperator.GormDBTable, 0)
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.DB.WithContext(ctx).
		Raw("SELECT  distinct  tb.schemaname as table_schema, " +
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

func (p PGOperator) GetColumns(ctx context.Context, dbName string) (dbTableColMap map[string]map[string]*dboperator.TableColInfo, err error) {
	dbTableColMap = make(map[string]map[string]*dboperator.TableColInfo, 0)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	gormTableColumns := make([]*dboperator.GormTableColumn, 0)
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.DB.WithContext(ctx).
		Raw("select distinct ic.table_schema table_schema," +
			"ic.table_name table_name, " +
			"ic.column_name as column_name," +
			"case" +
			"	when ic.udt_name='varchar' or ic.udt_name='character varying' then" +
			"		ic.udt_name || '(' || ic.character_maximum_length || ')'" +
			"	when ic.udt_name='numeric' or ic.udt_name='decimal' then" +
			"		ic.udt_name || '(' || ic.numeric_precision || ',' || ic.numeric_scale || ')'" +
			"	when ic.udt_name='timestamp' and ic.datetime_precision <> 0 then" +
			"		ic.udt_name || '(' || ic.datetime_precision || ')'" +
			"	else ic.udt_name" +
			"end as data_type," +
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

func (p PGOperator) GetColumnsUnderTables(ctx context.Context, dbName, logicDBName string, tableNames []string) (tableColMap map[string]*dboperator.TableColInfo, err error) {
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
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.DB.WithContext(ctx).
		Raw("select distinct ic.table_schema table_schema,"+
			"ic.table_name table_name, "+
			"ic.column_name as column_name,"+
			"case"+
			"	when ic.udt_name='varchar' or ic.udt_name='character varying' then"+
			"		ic.udt_name || '(' || ic.character_maximum_length || ')'"+
			"	when ic.udt_name='numeric' or ic.udt_name='decimal' then"+
			"		ic.udt_name || '(' || ic.numeric_precision || ',' || ic.numeric_scale || ')'"+
			"	when ic.udt_name='timestamp' and ic.datetime_precision <> 0 then"+
			"		ic.udt_name || '(' || ic.datetime_precision || ')'"+
			"	else ic.udt_name"+
			"end as data_type,d.description as comments,"+
			"case"+
			"    when ic.is_nullable = 'YES' then"+
			"        true"+
			"else"+
			"    false"+
			"end as is_nullable,"+
			"d.description as comments,"+
			"ic.ordinal_position "+
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

func (p PGOperator) CreateSchema(ctx context.Context, dbName, schemaName, commentInfo string) (err error) {
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	if commentInfo == "" {
		commentInfo = schemaName
	}
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.DB.WithContext(ctx).Exec("create schema if not exists " + utils.QuotaName(schemaName)).Error
	if err != nil {
		return
	}
	commentStr := fmt.Sprintf("comment on schema %s is '%s'", utils.QuotaName(schemaName), commentInfo)
	err = db.DB.WithContext(ctx).Exec(commentStr).Error
	if err != nil {
		return
	}
	return
}

func (p PGOperator) GetTablePrimeKeys(ctx context.Context, dbName string, schemaName string, tables []string) (primeKeyInfo map[string][]string, err error) {
	if dbName == "" || schemaName == "" || len(tables) == 0 {
		return
	}
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	primeKeyInfo = make(map[string][]string)
	tablePrimeKeys := make([]*dboperator.TablePrimeKey, 0)
	queryTables := make([]string, 0)
	for _, table := range tables {
		queryTables = append(queryTables, "'"+table+"'")
	}
	tableList := strings.Join(queryTables, ",")
	tableList = "(" + tableList + ")"

	err = db.DB.WithContext(ctx).Raw(`SELECT distinct tc.constraint_name as constraint_name,
      tc.constraint_type,kcu.table_schema as schema_name,
       kcu.TABLE_NAME as table_name, kcu.COLUMN_NAME as column_name
  FROM information_schema.table_constraints AS tc
  JOIN information_schema.key_column_usage  AS kcu
    ON tc.constraint_name = kcu.constraint_name
 WHERE tc.constraint_type = 'PRIMARY KEY' AND kcu.table_schema = '` + schemaName + `' AND kcu.Table_Name IN (` + strings.Join(queryTables, ",") + `)`).
		Scan(&tablePrimeKeys).Error

	if err != nil {
		return
	}
	for _, val := range tablePrimeKeys {
		primeKeyInfo[val.TableName] = append(primeKeyInfo[val.TableName], val.ColumnName)
	}
	return
}

func (p PGOperator) GetTableUniqueKeys(ctx context.Context, dbName string, schemaName string, tables []string) (uniqueKeyInfo map[string]map[string][]string, err error) {
	if dbName == "" || schemaName == "" || len(tables) == 0 {
		return
	}
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	uniqueKeyInfo = make(map[string]map[string][]string)
	tableUniqueKeys := make([]*dboperator.TablePrimeKey, 0)
	queryTables := make([]string, 0)
	for _, table := range tables {
		queryTables = append(queryTables, "'"+table+"'")
	}
	tableList := strings.Join(queryTables, ",")
	tableList = "(" + tableList + ")"
	err = db.DB.WithContext(ctx).Raw(`SELECT distinct tc.constraint_name as constraint_name,
      tc.constraint_type,kcu.table_schema as schema_name,
       kcu.TABLE_NAME as table_name, kcu.COLUMN_NAME as column_name
  FROM information_schema.table_constraints AS tc
  JOIN information_schema.key_column_usage  AS kcu
    ON tc.constraint_name = kcu.constraint_name
 WHERE tc.constraint_type = 'UNIQUE' AND kcu.table_schema = '` + schemaName + `' AND kcu.Table_Name IN (` + strings.Join(queryTables, ",") + `)`).
		Scan(&tableUniqueKeys).Error
	if err != nil {
		return
	}
	for _, val := range tableUniqueKeys {
		uniqueMap, ok := uniqueKeyInfo[val.TableName]
		if !ok {
			uniqueMap = make(map[string][]string)
		}
		uniqueMap[val.ConstraintName] = append(uniqueMap[val.ConstraintName], val.ColumnName)
		uniqueKeyInfo[val.TableName] = uniqueMap
	}
	return
}

func (p PGOperator) ExecuteDDL(ctx context.Context, dbName, schemaName string, primaryKeysMap map[string][]string, uniqueKeysMap map[string]map[string][]string, tableFieldsMap map[string][]*dboperator.Field) (ddlSQL string, err error) {
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}

	//ddlSQL := ""
	ddlTemplate := `
create table if not exists %s (
	%s 
);`
	for tableName, fields := range tableFieldsMap {
		var includeField string
		for _, field := range fields {
			if field == nil {
				continue
			}
			dataType := p.Trans2DataType(field)
			includeField += fmt.Sprintf("	%s %s,", utils.QuotaName(field.ColumnName), dataType) + fmt.Sprintln()
		}
		if len(primaryKeysMap) > 0 {
			keys := primaryKeysMap[tableName]
			for i, key := range keys {
				keys[i] = utils.QuotaName(key)
			}
			if len(keys) > 0 {
				includeField += fmt.Sprintf("	primary key (%s),", strings.Join(keys, ",")) + fmt.Sprintln()
			}
		}
		if len(uniqueKeysMap) > 0 {
			uniqueKeys := uniqueKeysMap[tableName]
			for _, columns := range uniqueKeys {
				for i, column := range columns {
					columns[i] = utils.QuotaName(column)
				}
				includeField += fmt.Sprintf("	unique (%s),", strings.Join(columns, ",")) + fmt.Sprintln()
			}
		}

		includeField = strings.TrimSpace(includeField)
		includeField = strings.Trim(includeField, ",")

		ddlStr := fmt.Sprintf(ddlTemplate, utils.QuotaName(tableName), includeField)
		ddlSQL += ddlStr + fmt.Sprintln()
	}

	err = db.DB.WithContext(ctx).Exec(ddlSQL).Error
	if err != nil {
		return
	}
	return
}
