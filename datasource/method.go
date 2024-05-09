package datasource

import (
	"context"

	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/dbx"
	"github.com/jasonlabz/dbutil/log"
)

func GenTable(ctx context.Context, source dbx.Config, target dbx.Config, sourceSchema, targetSchema string, tableNames []string) (string, error) {
	logger := log.GetLogger(ctx)
	sourceDBType := source.DBType
	source.DBName = "source"
	targetDBType := target.DBType
	target.DBName = "target"

	checkMap := map[string]bool{}
	for _, name := range tableNames {
		checkMap[name] = true
	}
	sourceDS, err := LoadDS(sourceDBType)
	if err != nil {
		logger.WithError(err).Error(err.Error())
		return "", err
	}
	err = sourceDS.Open(&source)
	if err != nil {
		logger.WithError(err).Error("数据库连接失败")
		return "", err
	}
	tableMap, err := sourceDS.GetTablesUnderSchema(ctx, source.DBName, []string{sourceSchema})
	if err != nil {
		logger.WithError(err).Error("数据库查询失败")
		return "", err
	}

	tables := make([]string, 0)
	for _, tableInfos := range tableMap {
		for _, tableInfo := range tableInfos.TableInfoList {
			tables = append(tables, tableInfo.TableName)
		}
	}
	columnsUnderTables, getColumnErr := sourceDS.GetColumnsUnderTable(ctx, source.DBName, sourceSchema, tables)
	if getColumnErr != nil {
		logger.WithError(getColumnErr).Error("get table column error")
		return "", err
	}

	tablePrimeKeys, err := sourceDS.GetTablePrimeKeys(ctx, source.DBName, sourceSchema, tables)
	if err != nil {
		logger.WithError(err).Error("GetTablePrimeKeys error")
		return "", err
	}

	tableUniqueKeys, err := sourceDS.GetTableUniqueKeys(ctx, source.DBName, sourceSchema, tables)
	if err != nil {
		logger.WithError(err).Error("GetTableUniqueKeys error")
		return "", err
	}

	targetDS, err := LoadDS(targetDBType)
	if err != nil {
		logger.WithError(err).Error(err.Error())
		return "", err
	}

	err = targetDS.Open(&target)
	if err != nil {
		logger.WithError(err).Error("数据库连接失败")
		return "", err
	}

	_ = targetDS.CreateSchema(ctx, target.DBName, targetSchema, "")

	fieldsMap := make(map[string][]*dboperator.Field)

	for _, info := range columnsUnderTables {
		tableName := info.TableName
		if len(checkMap) > 0 && !checkMap[tableName] {
			continue
		}

		fields := make([]*dboperator.Field, 0)
		for _, columnInfo := range info.ColumnInfoList {
			field := sourceDS.Trans2CommonField(columnInfo.DataType)
			if field == nil {
				continue
			}
			field.ColumnName = columnInfo.ColumnName
			fields = append(fields, field)
		}
		fieldsMap[tableName] = fields
	}
	ddlSQL, err := targetDS.ExecuteDDL(ctx, target.DBName, targetSchema, tablePrimeKeys, tableUniqueKeys, fieldsMap)
	if err != nil {
		logger.WithError(err).Error("execute ddl error")
		return "", err
	}
	return ddlSQL, nil
}
