// Package base -----------------------------
// @file      : Field.go
// @author    : jasonlabz
// @contact   : 1783022886@qq.com
// @time      : 2024/10/19 1:13
// -------------------------------------------
package base

import (
	"dagine/etl/element"
	"github.com/bytedance/sonic"
)

type Field struct {
	Key   string
	Value any
}

type MsgBody struct {
	Data    map[string]any `json:"data"`
	ExtInfo map[string]any `json:"ext_info"`
}

type ETLRecord struct {
	records []element.Record
	fields  []*Field
}

type ParamHandler interface {
	Transfer(inData any) ([]element.Record, []*Field)
}

type JSONParamHandler struct{}

func (j *JSONParamHandler) Transfer(inData any) (records []element.Record, fields []*Field) {
	if inData == nil {
		return
	}
	switch inData.(type) {
	case string:
		// json格式
		msgBody := MsgBody{}
		err := sonic.Unmarshal([]byte(inData.(string)), &msgBody)
		if err != nil {
			msgBody.Data = map[string]any{"content": inData.(string)}
		}
		record := element.NewDefaultRecord()
		for key, val := range msgBody.Data {
			column := element.NewDefaultColumn(element.NewColumnValue(val), key, element.ByteSize(val))
			record.Add(column)
		}
		if record.ColumnNumber() > 0 {
			records = append(records, record)
		}
	case *ETLRecord:
		etlRecord := inData.(*ETLRecord)
		records = etlRecord.records
		fields = etlRecord.fields
	}
	return
}
