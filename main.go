package dbutil

import (
	"context"
	"flag"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/dbx"
	"github.com/jasonlabz/dbutil/log"
)

type inputParam struct {
	Source       dbx.Config `json:"source"`       // 源库配置信息
	Target       dbx.Config `json:"target"`       // 目标库配置信息
	SourceSchema string     `json:"sourceSchema"` // 源库schema
	TargetSchema string     `json:"targetSchema"` // 目标库schema
	TableList    []string   `json:"tableList"`    // 源库目标表
}

func main() {
	ctx := context.Background()
	var params string
	//var dataxPath string
	var ddlSavePath string
	flag.StringVar(&params, "c", "", "源库配置信息以及目标库配置信息,{\"source\":{},\"target\":{}}")
	//flag.StringVar(&dataxPath, "d", "", "datax同步工具目录，可不填，则不执行datax任务")
	flag.StringVar(&ddlSavePath, "p", "", "ddl语句配置文件保存位置，默认不保存")
	//limit := flag.Int("l", 0, "拉取数量")
	flag.Parse()
	if params == "" {
		log.DefaultLogger().Fatal("请传入参数, 如: -c ''")
	}
	paramStruct := inputParam{}
	err := sonic.Unmarshal([]byte(params), &paramStruct)
	if err != nil {
		log.DefaultLogger().WithError(err).Fatal("解析参数失败")
	}

	sourceDBType := paramStruct.Source.DBType
	paramStruct.Source.DBName = "source"
	targetDBType := paramStruct.Target.DBType
	paramStruct.Target.DBName = "target"
	sourceDS, err := dboperator.LoadDS(sourceDBType)
	if err != nil {
		log.DefaultLogger().WithError(err).Fatal(err.Error())
	}
	err = sourceDS.Open(&paramStruct.Source)
	if err != nil {
		log.DefaultLogger().WithError(err).Fatal("数据库连接失败")
	}
	tableMap, err := sourceDS.GetTablesUnderSchema(ctx, paramStruct.Source.DBName, []string{paramStruct.SourceSchema})
	if err != nil {
		log.DefaultLogger().WithError(err).Fatal("数据库查询失败")
	}
	//db, err := sourceDS.GetDB(paramStruct.Source.DBName)
	//if err != nil {
	//	log.DefaultLogger().WithError(err).Fatal("get db error")
	//}

	tables := make([]string, 0)
	for _, tableInfos := range tableMap {
		for _, tableInfo := range tableInfos.TableInfoList {
			tables = append(tables, tableInfo.TableName)
		}
	}
	columnsUnderTables, getColumnErr := sourceDS.GetColumnsUnderTable(ctx, paramStruct.Source.DBName, paramStruct.SourceSchema, tables)
	if getColumnErr != nil {
		log.DefaultLogger().WithError(getColumnErr).Fatal("get table column error")
	}

	targetDS, err := dboperator.LoadDS(targetDBType)
	if err != nil {
		log.DefaultLogger().WithError(err).Fatal(err.Error())
	}
	ddlTemplate := `create if not exist table %s (%s)`
	for _, info := range columnsUnderTables {
		tableName := info.TableName
		var includeField string
		for _, columnInfo := range info.ColumnInfoList {
			field := sourceDS.Trans2CommonField(columnInfo.DataType)
			if field == nil {
				continue
			}
			dataType := targetDS.Trans2DataType(field)
			includeField += fmt.Sprintf("%s %s,", columnInfo.ColumnName, dataType)
		}
		ddlStr := fmt.Sprintf(ddlTemplate, fmt.Sprintf("%s.%s", paramStruct.TargetSchema, tableName), includeField)
		targetDS.ExecuteDDL(ctx, paramStruct.Target.DBName, ddlStr)
	}
}
