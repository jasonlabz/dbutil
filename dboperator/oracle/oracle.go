package oracle

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/dbx"
)

const DBTypeOracle dbx.DBType = dbx.DBTypeOracle

func NewOracleOperator() dboperator.IOperator {
	return &OracleOperator{}
}

type OracleOperator struct{}

func (o OracleOperator) GetDB(name string) (*dbx.DBWrapper, error) {
	return dbx.GetDB(name)
}

func (o OracleOperator) Open(config *dbx.Config) error {
	return dbx.InitConfig(config)
}

func (o OracleOperator) Ping(dbName string) error {
	return dbx.Ping(dbName)
}

func (o OracleOperator) Close(dbName string) error {
	return dbx.Close(dbName)
}

func (o OracleOperator) GetDataBySQL(ctx context.Context, dbName, sqlStatement string) (rows []map[string]interface{}, err error) {
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

func (o OracleOperator) GetTableData(ctx context.Context, dbName, schemaName, tableName string, pageInfo *dboperator.Pagination) (rows []map[string]interface{}, err error) {
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

func (o OracleOperator) GetTablesUnderSchema(ctx context.Context, dbName string, schemas []string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
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
		Raw("SELECT OWNER as table_schema, " +
			"TABLE_NAME as table_name, " +
			"COMMENTS as comments " +
			"FROM all_tab_comments " +
			"WHERE OWNER IN " +
			"(" + strings.Join(schemas, ",") + ") " +
			"ORDER BY OWNER, TABLE_NAME").
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

func (o OracleOperator) GetTablesUnderDB(ctx context.Context, dbName string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
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
		Raw("SELECT OWNER as table_schema, " +
			"TABLE_NAME as table_name, " +
			"COMMENTS as comments " +
			"FROM all_tab_comments " +
			"WHERE OWNER IN " +
			"(select SYS_CONTEXT('USERENV','CURRENT_SCHEMA') CURRENT_SCHEMA from dual) " +
			"ORDER BY OWNER, TABLE_NAME").
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

func (o OracleOperator) GetColumns(ctx context.Context, dbName string) (dbTableColMap map[string]map[string]*dboperator.TableColInfo, err error) {
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
		Raw("SELECT atc.OWNER as table_schema, " +
			"atc.TABLE_NAME as table_name, " +
			"atc.Column_Name as column_name," +
			" acc.COMMENTS as comments," +
			"atc.Data_TYPE  as data_type " +
			"FROM ALL_TAB_COLUMNS atc " +
			"left join all_col_comments acc " +
			"on acc.TABLE_NAME = atc.TABLE_NAME and acc.COLUMN_NAME = atc.COLUMN_NAME " +
			"WHERE atc.OWNER IN (select SYS_CONTEXT('USERENV','CURRENT_SCHEMA') CURRENT_SCHEMA from dual) " +
			"ORDER BY atc.OWNER, atc.TABLE_NAME, atc.Column_Name").
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

func (o OracleOperator) GetColumnsUnderTables(ctx context.Context, dbName, logicDBName string, tableNames []string) (tableColMap map[string]*dboperator.TableColInfo, err error) {
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
		Raw("SELECT atc.OWNER as table_schema, "+
			"atc.TABLE_NAME as table_name, "+
			"atc.Column_Name as column_name,"+
			" acc.COMMENTS as comments,"+
			"atc.Data_TYPE  as data_type "+
			"FROM ALL_TAB_COLUMNS atc "+
			"left join all_col_comments acc "+
			"on acc.TABLE_NAME = atc.TABLE_NAME and acc.COLUMN_NAME = atc.COLUMN_NAME "+
			"WHERE atc.OWNER = ? "+
			"AND atc.TABLE_NAME IN ? "+
			"ORDER BY atc.OWNER, atc.TABLE_NAME, atc.Column_Name", logicDBName, tableNames).
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

func (o OracleOperator) CreateSchema(ctx context.Context, dbName, schemaName, commentInfo string) (err error) {
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
	config, err := dbx.GetDBConfig(dbName)
	if err != nil {
		return
	}
	password := config.Password
	err = db.DB.WithContext(ctx).Exec(fmt.Sprintf("create user %s identified by %s", schemaName, password)).Error
	if err != nil {
		return
	}
	err = db.DB.WithContext(ctx).Exec(fmt.Sprintf("grant connect, resource to %s", schemaName)).Error
	if err != nil {
		return
	}
	return
}

func (o OracleOperator) GetTablePrimeKeys(ctx context.Context, dbName string, schemaName string, tables []string) (primeKeyInfo map[string][]string, err error) {
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
	err = db.DB.WithContext(ctx).Raw(`select cu.OWNER as schema_name, cu.TABLE_NAME as table_name,cu.CONSTRAINT_NAME as constraint_name,
    cu.COLUMN_NAME as column_name  from ALL_CONS_COLUMNS cu, ALL_CONSTRAINTS au where cu.CONSTRAINT_NAME = au.CONSTRAINT_NAME and cu.TABLE_NAME = au.TABLE_NAME 
    and cu.OWNER = au.OWNER and  au.OWNER = '` + schemaName + `' and 
    au.TABLE_NAME IN ` + tableList + ` and au.CONSTRAINT_TYPE = 'P'`).Scan(&tablePrimeKeys).Error
	if err != nil {
		return
	}

	for _, val := range tablePrimeKeys {
		primeKeyInfo[val.TableName] = append(primeKeyInfo[val.TableName], val.ColumnName)
	}
	return
}

func (o OracleOperator) GetTableUniqueKeys(ctx context.Context, dbName string, schemaName string, tables []string) (uniqueKeyInfo map[string]map[string][]string, err error) {
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
	err = db.DB.WithContext(ctx).Raw(`select cu.OWNER as schema_name, cu.TABLE_NAME as table_name,cu.CONSTRAINT_NAME as constraint_name,
    cu.COLUMN_NAME as column_name  from ALL_CONS_COLUMNS cu, ALL_CONSTRAINTS au where cu.CONSTRAINT_NAME = au.CONSTRAINT_NAME and cu.TABLE_NAME = au.TABLE_NAME 
    and cu.OWNER = au.OWNER and  au.OWNER = '` + schemaName + `' and 
    au.TABLE_NAME IN ` + tableList + ` and au.CONSTRAINT_TYPE = 'U'`).Scan(&tableUniqueKeys).Error
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

func (o OracleOperator) ExecuteDDL(ctx context.Context, dbName, schemaName string, primaryKeysMap map[string][]string,
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
			dataType := o.Trans2DataType(field)
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
	err := dboperator.RegisterDS(DBTypeOracle, NewOracleOperator())
	if err != nil {
		panic(err)
	}
}
