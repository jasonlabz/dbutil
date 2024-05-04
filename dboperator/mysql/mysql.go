package mysql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/dbx"
	"strings"
)

const DBTypeMYSQL dbx.DBType = dbx.DBTypeMySQL

func NewMySQLOperator() dboperator.IOperator {
	return &MySQLOperator{}
}

type MySQLOperator struct{}

var (
	TableNameAllTables     = "INFORMATION_SCHEMA.TABLES"
	TableNameAllTablesCols = "INFORMATION_SCHEMA.COLUMNS"
	ViewNameAllViews       = "INFORMATION_SCHEMA.VIEWS"
	ExcludeSchemes         = "('mysql', 'sys', 'performance_schema', 'information_schema')"
)

func (m MySQLOperator) GetDB(name string) (*dbx.DBWrapper, error) {
	return dbx.GetDB(name)
}

func (m MySQLOperator) Open(config *dbx.Config) error {
	return dbx.InitConfig(config)
}

func (m MySQLOperator) Ping(dbName string) error {
	return dbx.Ping(dbName)
}

func (m MySQLOperator) Close(dbName string) error {
	return dbx.Close(dbName)
}

func (m MySQLOperator) GetDataBySQL(ctx context.Context, dbName, sqlStatement string) (rows []map[string]interface{}, err error) {
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

func (m MySQLOperator) GetTableData(ctx context.Context, dbName, schemaName, tableName string, pageInfo *dboperator.Pagination) (rows []map[string]interface{}, err error) {
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

func (m MySQLOperator) GetTablesUnderSchema(ctx context.Context, dbName string, schemas []string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
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
		Raw("SELECT TABLE_SCHEMA as table_schema, " +
			"TABLE_NAME as table_name, " +
			"TABLE_COMMENT as comments " +
			"FROM INFORMATION_SCHEMA.TABLES " +
			"WHERE TABLE_TYPE = 'BASE TABLE' " +
			"AND TABLE_SCHEMA IN (" + strings.Join(schemas, ",") + ") " +
			"ORDER  BY TABLE_SCHEMA, TABLE_NAME").
		Find(&gormDBTables).Error
	if err != nil {
		return
	}
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

func (m MySQLOperator) GetTablesUnderDB(ctx context.Context, dbName string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
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
		Raw("SELECT TABLE_SCHEMA as table_schema, " +
			"TABLE_NAME as table_name, " +
			"TABLE_COMMENT as comments " +
			"FROM INFORMATION_SCHEMA.TABLES " +
			"WHERE TABLE_TYPE = 'BASE TABLE' " +
			"AND TABLE_SCHEMA NOT IN ('mysql', 'sys', 'performance_schema', 'information_schema') " +
			"ORDER  BY TABLE_SCHEMA, TABLE_NAME").
		Find(&gormDBTables).Error
	if err != nil {
		return
	}
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

func (m MySQLOperator) GetColumns(ctx context.Context, dbName string) (dbTableColMap map[string]map[string]*dboperator.TableColInfo, err error) {
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
		Raw("select " +
			"t.TABLE_SCHEMA table_schema, " +
			"t.TABLE_NAME table_name, " +
			"c.COLUMN_NAME column_name, " +
			"c.COLUMN_COMMENT comments, " +
			"c.COLUMN_TYPE data_type " +
			"from " +
			"INFORMATION_SCHEMA.TABLES t " +
			"inner join INFORMATION_SCHEMA.COLUMNS c on " +
			"t.TABLE_NAME = c.TABLE_NAME " +
			"and t.TABLE_SCHEMA = c.TABLE_SCHEMA " +
			"where " +
			"t.TABLE_TYPE = 'BASE TABLE' " +
			"AND t.TABLE_SCHEMA NOT IN ('mysql', 'sys', 'performance_schema', 'information_schema') " +
			"ORDER BY t.TABLE_NAME, c.COLUMN_NAME").
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

func (m MySQLOperator) GetColumnsUnderTables(ctx context.Context, dbName, logicDBName string, tableNames []string) (tableColMap map[string]*dboperator.TableColInfo, err error) {
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
		Raw("select "+
			"t.TABLE_SCHEMA table_schema, "+
			"t.TABLE_NAME table_name, "+
			"c.COLUMN_NAME column_name, "+
			"c.COLUMN_COMMENT comments, "+
			"c.COLUMN_TYPE data_type "+
			"from "+
			"INFORMATION_SCHEMA.TABLES t "+
			"inner join INFORMATION_SCHEMA.COLUMNS c on "+
			"t.TABLE_NAME = c.TABLE_NAME "+
			"and t.TABLE_SCHEMA = c.TABLE_SCHEMA "+
			"where "+
			"t.TABLE_TYPE = 'BASE TABLE' "+
			"AND t.TABLE_SCHEMA = ? "+
			"AND t.TABLE_NAME IN ? "+
			"ORDER BY t.TABLE_NAME, c.COLUMN_NAME", logicDBName, tableNames).
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

func (m MySQLOperator) CreateSchema(ctx context.Context, dbName, schemaName, commentInfo string) (err error) {
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
	err = db.DB.WithContext(ctx).Exec("create schema if not exists " + schemaName).Error
	if err != nil {
		return
	}
	return
}

func (m MySQLOperator) GetTablePrimeKeys(ctx context.Context, dbName string, schemaName string, tables []string) (primeKeyInfo map[string][]string, err error) {
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
	err = db.DB.WithContext(ctx).Raw(`select cu.TABLE_SCHEMA as schema_name, cu.TABLE_NAME as table_name,cu.CONSTRAINT_NAME as constraint_name,
    cu.COLUMN_NAME as column_name  from INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu where TABLE_SCHEMA = '` + schemaName + `' and 
    cu.Table_Name IN ` + tableList + ` and CONSTRAINT_NAME = 'PRIMARY'`).Scan(&tablePrimeKeys).Error
	if err != nil {
		return
	}
	for _, val := range tablePrimeKeys {
		primeKeyInfo[val.TableName] = append(primeKeyInfo[val.TableName], val.ColumnName)
	}
	return
}

func (m MySQLOperator) GetTableUniqueKeys(ctx context.Context, dbName string, schemaName string, tables []string) (uniqueKeyInfo map[string]map[string][]string, err error) {
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
		`SELECT k.CONSTRAINT_NAME,k.TABLE_SCHEMA,k.TABLE_NAME,k.COLUMN_NAME
FROM information_schema.table_constraints t
JOIN information_schema.key_column_usage k
USING(constraint_name,table_schema,table_name)
WHERE t.constraint_type='UNIQUE' and  TABLE_SCHEMA = '` + schemaName + `' and 
    cu.Table_Name IN ` + tableList).Scan(&tableUniqueKeys).Error
	if err != nil {
		return
	}
	for _, val := range tableUniqueKeys {
		uniqueMap, ok := uniqueKeyInfo[val.TableName]
		if !ok {
			uniqueMap = make(map[string][]string)
		}
		uniqueMap[val.IndexName] = append(uniqueMap[val.IndexName], val.ColumnName)
		uniqueKeyInfo[val.TableName] = uniqueMap
	}
	return
}

func (m MySQLOperator) ExecuteDDL(ctx context.Context, dbName, schemaName string, primaryKeysMap map[string][]string,
	uniqueKeysMap map[string]map[string][]string, tableFieldsMap map[string][]*dboperator.Field) (ddlSQL string, err error) {
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}

	//ddlSQL := ""
	ddlTemplate := `create if not exist table "%s" (
						%s 
					)`
	for tableName, fields := range tableFieldsMap {
		var includeField string
		for _, field := range fields {
			if field == nil {
				continue
			}
			dataType := m.Trans2DataType(field)
			includeField += fmt.Sprintf("%s %s,", field.ColumnName, dataType) + fmt.Sprintln()
		}
		if len(primaryKeysMap) > 0 {
			keys := primaryKeysMap[tableName]
			if len(keys) > 0 {
				includeField += fmt.Sprintf("primary key (%s),", strings.Join(keys, ",")) + fmt.Sprintln()
			}
		}
		if len(uniqueKeysMap) > 0 {
			uniqueKeys := uniqueKeysMap[tableName]
			for _, columns := range uniqueKeys {
				includeField += fmt.Sprintf("unique (%s),", strings.Join(columns, ",")) + fmt.Sprintln()
			}
		}

		includeField = strings.TrimSpace(includeField)
		includeField = strings.Trim(includeField, ",")

		ddlStr := fmt.Sprintf(ddlTemplate, tableName, includeField)
		ddlSQL += ddlStr + fmt.Sprintln()
	}

	err = db.DB.WithContext(ctx).Exec(ddlSQL).Error
	if err != nil {
		return
	}
	return
}

func init() {
	err := dboperator.RegisterDS(DBTypeMYSQL, NewMySQLOperator())
	if err != nil {
		panic(err)
	}
}
