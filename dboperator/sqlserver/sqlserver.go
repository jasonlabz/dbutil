package sqlserver

import (
	"context"
	"errors"
	"fmt"
	"github.com/jasonlabz/dbutil/core/utils"
	"strings"

	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/dbx"
)

func NewSqlserverOperator() dboperator.IOperator {
	return &SqlServerOperator{}
}

type SqlServerOperator struct{}

func (s SqlServerOperator) GetDB(name string) (*dbx.DBWrapper, error) {
	return dbx.GetDB(name)
}

func (s SqlServerOperator) Open(config *dbx.Config) error {
	return dbx.InitConfig(config)
}

func (s SqlServerOperator) Ping(dbName string) error {
	return dbx.Ping(dbName)
}

func (s SqlServerOperator) Close(dbName string) error {
	return dbx.Close(dbName)
}

func (s SqlServerOperator) GetDataBySQL(ctx context.Context, dbName, sqlStatement string) (rows []map[string]interface{}, err error) {
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

func (s SqlServerOperator) GetTableData(ctx context.Context, dbName, schemaName, tableName string, pageInfo *dboperator.Pagination) (rows []map[string]interface{}, err error) {
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

func (s SqlServerOperator) GetTablesUnderSchema(ctx context.Context, dbName string, schemas []string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
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
	db.DB.WithContext(ctx).
		Raw("select  " +
			"a.name AS table_name, " +
			"b.name as table_schema, " +
			"CONVERT(NVARCHAR(100),isnull(c.[value],'-')) AS comments " +
			"FROM sys.tables a " +
			"LEFT JOIN sys.schemas b " +
			"ON a.schema_id = b.schema_id " +
			"LEFT JOIN sys.extended_properties c " +
			"ON (a.object_id = c.major_id AND c.minor_id = 0) " +
			"WHERE b.name IN (" + strings.Join(schemas, ",") + ") " +
			"ORDER BY b.name,a.name").
		Find(&gormDBTables)
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

func (s SqlServerOperator) GetTablesUnderDB(ctx context.Context, dbName string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
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
	db.DB.WithContext(ctx).
		Raw("select  " +
			"a.name AS table_name, " +
			"b.name as table_schema, " +
			"CONVERT(NVARCHAR(100),isnull(c.[value],'-')) AS comments " +
			"FROM sys.tables a " +
			"LEFT JOIN sys.schemas b " +
			"ON a.schema_id = b.schema_id " +
			"LEFT JOIN sys.extended_properties c " +
			"ON (a.object_id = c.major_id AND c.minor_id = 0) " +
			"WHERE b.name not like 'db_%' and  b.name NOT IN ('sys','INFORMATION_SCHEMA') " +
			"ORDER BY b.name,a.name").
		Find(&gormDBTables)
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

func (s SqlServerOperator) GetColumns(ctx context.Context, dbName string) (dbTableColMap map[string]map[string]*dboperator.TableColInfo, err error) {
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
	db.DB.WithContext(ctx).
		Raw("SELECT TABLE_SCHEMA as table_schema, " +
			"TABLE_NAME as table_name, " +
			"COLUMN_NAME as column_name, " +
			"DATA_TYPE as data_type " +
			"FROM INFORMATION_SCHEMA.Columns " +
			"WHERE TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA') " +
			"ORDER BY TABLE_NAME, COLUMN_NAME").
		Find(&gormTableColumns)
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

func (s SqlServerOperator) GetColumnsUnderTables(ctx context.Context, dbName, logicDBName string, tableNames []string) (tableColMap map[string]*dboperator.TableColInfo, err error) {
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
	db.DB.WithContext(ctx).
		Raw("SELECT TABLE_SCHEMA as table_schema, "+
			"TABLE_NAME as table_name, "+
			"COLUMN_NAME as column_name, "+
			"DATA_TYPE as data_type "+
			"FROM INFORMATION_SCHEMA.Columns "+
			"WHERE TABLE_SCHEMA = ? "+
			"AND TABLE_NAME IN ? "+
			"ORDER BY TABLE_NAME, COLUMN_NAME", logicDBName, tableNames).
		Find(&gormTableColumns)
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

func (s SqlServerOperator) CreateSchema(ctx context.Context, dbName, schemaName, commentInfo string) (err error) {
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
	err = db.DB.WithContext(ctx).Exec(fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '%s')
    EXEC sp_executesql N'CREATE SCHEMA %s'`, schemaName, schemaName)).Error
	if err != nil {
		return
	}
	return
}

func (s SqlServerOperator) GetTablePrimeKeys(ctx context.Context, dbName string, schemaName string, tables []string) (primeKeyInfo map[string][]string, err error) {
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
	err = db.DB.WithContext(ctx).Raw(`SELECT
    sc.name as schema_name,
    t.name AS table_name,
    c.name AS column_name,
    k.name AS constraint_name
FROM
    sys.indexes k
JOIN
    sys.tables t ON k.object_id = t.object_id
JOIN
    sys.index_columns ic ON k.object_id = ic.object_id AND k.index_id = ic.index_id
JOIN
    sys.columns c ON ic.object_id = c.object_id AND c.column_id = ic.column_id
JOIN sys.schemas sc on t.schema_id = sc.schema_id

WHERE
    k.is_primary_key = 1 AND sc.name = '` + schemaName + `' and 
    t.name IN ` + tableList).Scan(&tablePrimeKeys).Error
	if err != nil {
		return
	}
	for _, val := range tablePrimeKeys {
		primeKeyInfo[val.TableName] = append(primeKeyInfo[val.TableName], val.ColumnName)
	}
	return
}

func (s SqlServerOperator) GetTableUniqueKeys(ctx context.Context, dbName string, schemaName string, tables []string) (uniqueKeyInfo map[string]map[string][]string, err error) {
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
	err = db.DB.WithContext(ctx).Raw(
		`SELECT
    sc.name as schema_name,
    t.name AS table_name,
    c.name AS column_name,
    k.name AS constraint_name
FROM
    sys.indexes k
JOIN
    sys.tables t ON k.object_id = t.object_id
JOIN
    sys.index_columns ic ON k.object_id = ic.object_id AND k.index_id = ic.index_id
JOIN
    sys.columns c ON ic.object_id = c.object_id AND c.column_id = ic.column_id
JOIN
    sys.schemas sc on t.schema_id = sc.schema_id
WHERE
    k.is_unique_constraint = 1 AND sc.name = '` + schemaName + `' AND  
    t.name IN ` + tableList).Scan(&tableUniqueKeys).Error
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

func (s SqlServerOperator) ExecuteDDL(ctx context.Context, dbName, schemaName string, primaryKeysMap map[string][]string, uniqueKeysMap map[string]map[string][]string, tableFieldsMap map[string][]*dboperator.Field) (ddlSQL string, err error) {
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
if not exists (select * from sysobjects where name = '%s' and xtype= 'U')
create table %s (
    %s
);`
	for tableName, fields := range tableFieldsMap {
		var includeField string
		for _, field := range fields {
			if field == nil {
				continue
			}
			dataType := s.Trans2DataType(field)
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

		ddlStr := fmt.Sprintf(ddlTemplate, tableName, utils.QuotaName(tableName), includeField)
		ddlSQL += ddlStr + fmt.Sprintln()
	}

	err = db.DB.WithContext(ctx).Exec(ddlSQL).Error
	if err != nil {
		return
	}
	return
}
