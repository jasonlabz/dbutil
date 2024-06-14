package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jasonlabz/dbutil/core/utils"
	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/dbx"
)

func NewSQLiteOperator() dboperator.IOperator {
	return &SQLiteOperator{}
}

type SQLiteOperator struct{}

var (
	TableNameAllTables     = "INFORMATION_SCHEMA.TABLES"
	TableNameAllTablesCols = "INFORMATION_SCHEMA.COLUMNS"
	ViewNameAllViews       = "INFORMATION_SCHEMA.VIEWS"
	ExcludeSchemes         = "('mysql', 'sys', 'performance_schema', 'information_schema')"
)

func (s SQLiteOperator) GetDB(name string) (*dbx.DBWrapper, error) {
	return dbx.GetDB(name)
}

func (s SQLiteOperator) Open(config *dbx.Config) error {
	return dbx.InitConfig(config)
}

func (s SQLiteOperator) Ping(dbName string) error {
	return dbx.Ping(dbName)
}

func (s SQLiteOperator) Close(dbName string) error {
	return dbx.Close(dbName)
}

func (s SQLiteOperator) GetDataBySQL(ctx context.Context, dbName, sqlStatement string) (rows []map[string]interface{}, err error) {
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

func (s SQLiteOperator) GetTableData(ctx context.Context, dbName, schemaName, tableName string, pageInfo *dboperator.Pagination) (rows []map[string]interface{}, err error) {
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
	tx := db.DB.WithContext(ctx).
		Table(queryTable)
	if pageInfo != nil {
		tx = tx.Count(&count).
			Offset(int(pageInfo.GetOffset())).
			Limit(int(pageInfo.PageSize))
	}
	err = tx.Scan(&rows).Error
	pageInfo.Total = count
	pageInfo.SetPageCount()
	return
}

func (s SQLiteOperator) GetTablesUnderSchema(ctx context.Context, dbName string, schemas []string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
	dbTableMap = make(map[string]*dboperator.LogicDBInfo)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	defaultName := "sqlite_default"
	gormDBTables := make([]*dboperator.GormDBTable, 0)
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.DB.WithContext(ctx).
		Raw("SELECT name as table_name" +
			"FROM sqlite_master " +
			"WHERE type = 'table'").
		Find(&gormDBTables).Error
	if err != nil {
		return
	}
	if len(gormDBTables) == 0 {
		return
	}
	for _, row := range gormDBTables {
		if logicDBInfo, ok := dbTableMap[defaultName]; !ok {
			dbTableMap[defaultName] = &dboperator.LogicDBInfo{
				SchemaName: defaultName,
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

func (s SQLiteOperator) GetTablesUnderDB(ctx context.Context, dbName string) (dbTableMap map[string]*dboperator.LogicDBInfo, err error) {
	dbTableMap = make(map[string]*dboperator.LogicDBInfo)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	defaultName := "sqlite_default"
	gormDBTables := make([]*dboperator.GormDBTable, 0)
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	err = db.DB.WithContext(ctx).
		Raw("SELECT name as table_name" +
			"FROM sqlite_master " +
			"WHERE type = 'table'").
		Find(&gormDBTables).Error
	if err != nil {
		return
	}
	if len(gormDBTables) == 0 {
		return
	}
	for _, row := range gormDBTables {
		if logicDBInfo, ok := dbTableMap[defaultName]; !ok {
			dbTableMap[defaultName] = &dboperator.LogicDBInfo{
				SchemaName: defaultName,
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

func (s SQLiteOperator) GetColumns(ctx context.Context, dbName string) (dbTableColMap map[string]map[string]*dboperator.TableColInfo, err error) {
	dbTableColMap = make(map[string]map[string]*dboperator.TableColInfo, 0)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	sqliteTableColumn := make([]*dboperator.SQLiteTableColumn, 0)
	tableMap, err := s.GetTablesUnderDB(ctx, dbName)
	if err != nil {
		return
	}
	defaultName := "sqlite_default"

	for _, schemaTableInfo := range tableMap {
		for _, tableInfo := range schemaTableInfo.TableInfoList {
			db, err := dbx.GetDB(dbName)
			if err != nil {
				return
			}
			err = db.DB.WithContext(ctx).
				Raw("PRAGMA table_info(?)", tableInfo.TableName).
				Find(&sqliteTableColumn).Error
			if err != nil {
				return
			}
			if len(sqliteTableColumn) == 0 {
				return
			}

			for _, row := range sqliteTableColumn {
				if dbTableColInfoMap, ok := dbTableColMap[defaultName]; !ok {
					dbTableColMap[defaultName] = map[string]*dboperator.TableColInfo{
						tableInfo.TableName: {
							TableName: tableInfo.TableName,
							ColumnInfoList: []*dboperator.ColumnInfo{{
								ColumnName: row.ColumnName,
								DataType:   row.DataType,
							}},
						},
					}
				} else if tableColInfo, ok_ := dbTableColInfoMap[tableInfo.TableName]; !ok_ {
					dbTableColInfoMap[tableInfo.TableName] = &dboperator.TableColInfo{
						TableName: tableInfo.TableName,
						ColumnInfoList: []*dboperator.ColumnInfo{{
							ColumnName: row.ColumnName,
							DataType:   row.DataType,
						}},
					}
				} else {
					tableColInfo.ColumnInfoList = append(tableColInfo.ColumnInfoList, &dboperator.ColumnInfo{
						ColumnName: row.ColumnName,
						DataType:   row.DataType,
					})
				}
			}
		}

	}

	return
}

func (s SQLiteOperator) GetColumnsUnderTables(ctx context.Context, dbName, schemaName string, tableNames []string) (tableColMap map[string]*dboperator.TableColInfo, err error) {
	tableColMap = make(map[string]*dboperator.TableColInfo, 0)
	if dbName == "" {
		err = errors.New("empty dnName")
		return
	}
	if len(tableNames) == 0 {
		err = errors.New("empty tableNames")
		return
	}

	sqliteTableColumns := make([]*dboperator.SQLiteTableColumn, 0)
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	for _, table := range tableNames {
		db.DB.WithContext(ctx).
			Raw("PRAGMA table_info(?)", table).
			Find(&sqliteTableColumns)
		if len(sqliteTableColumns) == 0 {
			continue
		}

		for _, row := range sqliteTableColumns {
			if tableColInfo, ok := tableColMap[table]; !ok {
				tableColMap[table] = &dboperator.TableColInfo{
					TableName: table,
					ColumnInfoList: []*dboperator.ColumnInfo{{
						ColumnName: row.ColumnName,
						DataType:   row.DataType,
					}},
				}
			} else {
				tableColInfo.ColumnInfoList = append(tableColInfo.ColumnInfoList, &dboperator.ColumnInfo{
					ColumnName: row.ColumnName,
					DataType:   row.DataType,
				})
			}
		}
	}

	return
}

func (s SQLiteOperator) CreateSchema(ctx context.Context, dbName, schemaName, commentInfo string) (err error) {
	//if dbName == "" {
	//	err = errors.New("empty dnName")
	//	return
	//}
	//if commentInfo == "" {
	//	commentInfo = schemaName
	//}
	//db, err := dbx.GetDB(dbName)
	//if err != nil {
	//	return
	//}
	//err = db.DB.WithContext(ctx).Exec("create schema if not exists " + schemaName).Error
	//if err != nil {
	//	return
	//}
	return
}

func (s SQLiteOperator) GetTablePrimeKeys(ctx context.Context, dbName string, schemaName string, tables []string) (primeKeyInfo map[string][]string, err error) {
	if dbName == "" || schemaName == "" || len(tables) == 0 {
		return
	}
	db, err := dbx.GetDB(dbName)
	if err != nil {
		return
	}
	primeKeyInfo = make(map[string][]string)
	sqliteTableColumns := make([]*dboperator.SQLiteTableColumn, 0)
	for _, table := range tables {
		db.DB.WithContext(ctx).
			Raw("PRAGMA table_info(?)", table).
			Find(&sqliteTableColumns)
		if len(sqliteTableColumns) == 0 {
			continue
		}

		for _, row := range sqliteTableColumns {
			if row.PrimaryKey == 1 {
				primeKeyInfo[table] = append(primeKeyInfo[table], row.ColumnName)
			}
		}
	}
	return
}

func (s SQLiteOperator) GetTableUniqueKeys(ctx context.Context, dbName string, schemaName string, tables []string) (uniqueKeyInfo map[string]map[string][]string, err error) {
	uniqueKeyInfo = make(map[string]map[string][]string)
	//if dbName == "" || schemaName == "" || len(tables) == 0 {
	//	return
	//}
	//db, err := dbx.GetDB(dbName)
	//if err != nil {
	//	return
	//}
	//uniqueKeyInfo = make(map[string]map[string][]string)
	//tableUniqueKeys := make([]*dboperator.TablePrimeKey, 0)
	//sqliteTableColumns := make([]*dboperator.SQLiteTableColumn, 0)
	//for _, table := range tables {
	//	db.DB.WithContext(ctx).
	//		Raw("PRAGMA table_info(?)", table).
	//		Find(&sqliteTableColumns)
	//	if len(sqliteTableColumns) == 0 {
	//		continue
	//	}
	//
	//	for _, row := range sqliteTableColumns {
	//		if row.PrimaryKey == 1 {
	//			uniqueKeyInfo[table] = append(primeKeyInfo[table], row.ColumnName)
	//		}
	//	}
	//}
	//for _, val := range tableUniqueKeys {
	//	uniqueMap, ok := uniqueKeyInfo[val.TableName]
	//	if !ok {
	//		uniqueMap = make(map[string][]string)
	//	}
	//	uniqueMap[val.ConstraintName] = append(uniqueMap[val.ConstraintName], val.ColumnName)
	//	uniqueKeyInfo[val.TableName] = uniqueMap
	//}
	return
}

func (s SQLiteOperator) ExecuteDDL(ctx context.Context, dbName, schemaName string, primaryKeysMap map[string][]string,
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

		includeField = strings.TrimSpace(includeField)
		includeField = strings.Trim(includeField, ",")

		ddlStr := fmt.Sprintf(ddlTemplate, fmt.Sprintf("%s.%s", utils.QuotaName(schemaName), utils.QuotaName(tableName)), includeField)
		ddlSQL += ddlStr + fmt.Sprintln()
	}

	err = db.DB.WithContext(ctx).Exec(ddlSQL).Error
	if err != nil {
		return
	}
	return
}
