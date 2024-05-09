package main

import (
	"context"
	"errors"
	"flag"
	"os"

	"github.com/bytedance/sonic"
	"github.com/jasonlabz/dbutil/core/utils"
	"github.com/jasonlabz/dbutil/datasource"
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

func (i inputParam) validateParam() error {
	if i.SourceSchema == "" {
		return errors.New("请配置sourceSchema")
	}
	if i.TargetSchema == "" {
		return errors.New("请配置targetSchema")
	}
	if i.Source.DSN == "" && i.Source.Host == "" {
		return errors.New("请配置源库DSN或者host")
	}
	if i.Target.DSN == "" && i.Target.Host == "" {
		return errors.New("请配置目标库DSN或者host")
	}
	return nil
}

func main() {
	ctx := context.Background()
	var params string
	var ddlSavePath string
	//var skip bool
	filePath := "./conf.json"
	exist := utils.IsExist(filePath)

	if !exist {
		flag.StringVar(&params, "c", "", "源库配置信息以及目标库配置信息,{\"source\":{},\"target\":{}}")
		flag.StringVar(&ddlSavePath, "p", "", "ddl语句配置文件保存位置，默认不保存")
		//flag.BoolVar(&skip, "s", false, "ddl语句配置文件保存位置，默认不保存")
		flag.Parse()
	} else {
		file, err := os.ReadFile(filePath)
		if err != nil {
			log.DefaultLogger().Fatal("请配置conf.json")
		}
		params = string(file)
	}

	if params == "" {
		log.DefaultLogger().Fatal("请传入参数, 如: -c ''")
	}
	paramStruct := inputParam{}
	err := sonic.Unmarshal([]byte(params), &paramStruct)
	if err != nil {
		log.DefaultLogger().WithError(err).Fatal("解析参数失败")
	}
	err = paramStruct.validateParam()
	if err != nil {
		log.DefaultLogger().WithError(err).Fatal("解析参数失败")
	}

	ddlSQL, err := datasource.GenTable(ctx, paramStruct.Source, paramStruct.Target, paramStruct.SourceSchema, paramStruct.TargetSchema, paramStruct.TableList)
	if err != nil {
		log.DefaultLogger().WithError(err).Fatal("gen table error")
	}
	//if ddlSavePath != "" && utils.IsExist(ddlSavePath) {
	if ddlSavePath != "" {
		f, openErr := os.OpenFile(ddlSavePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if openErr != nil {
			log.DefaultLogger().WithError(openErr).Fatal("openErr error")
		}
		defer f.Close()
		_, writeErr := f.WriteString(ddlSQL)
		if writeErr != nil {
			log.DefaultLogger().WithError(writeErr).Fatal("writeErr error")
		}
	}
}
