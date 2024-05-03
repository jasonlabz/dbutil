package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jasonlabz/dbutil/core/utils"
	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/log"
)

func (m MySQLOperator) Trans2CommonField(dataType string) *dboperator.Field {
	var field dboperator.Field
	lowerWords := strings.ToLower(dataType)
	typeStr := lowerWords
	var extra []string
	if strings.Contains(lowerWords, ")") {
		lIndex := strings.Index(lowerWords, "(")
		rIndex := strings.Index(lowerWords, ")")
		if lIndex != -1 {
			typeStr = lowerWords[:lIndex]
			extraStr := lowerWords[lIndex+1 : rIndex]
			extra = strings.Split(extraStr, ",")
			for i, s := range extra {
				extra[i] = strings.TrimSpace(s)
			}
		}
	}

	switch typeStr {
	case "char", "varchar", "tinytext", "character":
		// data_type: varchar、varchar(100)、etc...
		field = *dboperator.StringField
	case "date", "time", "year", "datetime", "timestamp":
		field = *dboperator.TimeField
		switch typeStr {
		case "date":
			field.TimeType = "date"
		case "time":
			field.TimeType = "time"
		case "year":
			field.TimeType = "year"
		case "datetime":
			field.TimeType = "datetime"
		case "timestamp":
			field.TimeType = "timestamp"
		}
	case "mediumtext", "text", "longtext":
		field = *dboperator.StringField
		field.IsText = true
	case "tinyint", "int1":
		field = *dboperator.Int8Field
	case "smallint", "int2":
		field = *dboperator.Int16Field
	case "mediumint", "int", "integer", "int3", "int4":
		field = *dboperator.Int32Field
	case "bigint", "int8":
		field = *dboperator.Int64Field
	case "float", "float4":
		field = *dboperator.Float32Field
	case "numeric", "decimal", "number":
		field = *dboperator.Float64Field
		field.IsFixedNumber = true
	case "float8", "double":
		field = *dboperator.Float64Field
	case "boolean", "bool", "bit":
		field = *dboperator.BoolField
	case "tinyblob", "blob", "mediumblob", "longblob":
		field = *dboperator.BytesField
	default:
		log.DefaultLogger().Warn("handle with default mysql type:%s", dataType)
		field = *dboperator.StringField
		field.IsText = true
	}
	if len(extra) == 1 {
		val, err := strconv.Atoi(extra[0])
		if err != nil {
			field.Precision, field.Length = -1, -1
		} else {
			field.Precision, field.Length = val, val
		}
	}

	if len(extra) == 2 {
		val1, err1 := strconv.Atoi(extra[1])
		val0, err0 := strconv.Atoi(extra[0])
		if err1 != nil {
			field.Scale = -1
		} else {
			field.Scale = val1
		}
		if err0 != nil {
			field.Precision = -1
		} else {
			field.Precision = val0
		}
	}
	return &field
}

func (m MySQLOperator) Trans2DataType(field *dboperator.Field) string {
	switch field.Type {
	case dboperator.BYTES:
		fallthrough
	case dboperator.RUNES:
		return "longblob"
	case dboperator.INT8:
		return utils.IsTrueOrNot(field.Precision <= 0, "tinyint", fmt.Sprintf("tinyint(%d)", field.Precision))
	case dboperator.INT16:
		return utils.IsTrueOrNot(field.Precision <= 0, "smallint", fmt.Sprintf("tinyint(%d)", field.Precision))
	case dboperator.INT32:
		return utils.IsTrueOrNot(field.Precision <= 0, "int", fmt.Sprintf("int(%d)", field.Precision))
	case dboperator.INT64:
		return utils.IsTrueOrNot(field.Precision <= 0, "bigint", fmt.Sprintf("bigint(%d)", field.Precision))
	case dboperator.FLOAT32:
		return fmt.Sprintf("float%s", getTypeSuffix(field))
	case dboperator.FLOAT64:
		if field.IsFixedNumber {
			return fmt.Sprintf("decimal%s", getTypeSuffix(field))
		}
		return fmt.Sprintf("double%s", getTypeSuffix(field))
	case dboperator.BOOL:
		return "boolean"
	case dboperator.STRING:
		if field.IsText {
			return "text"
		}
		return utils.IsTrueOrNot(field.Length == 0, "varchar", fmt.Sprintf("varchar(%d)", field.Length))
	case dboperator.TIME:
		var timeType string
		switch field.TimeType {
		case "date":
			timeType = "date"
		case "time":
			timeType = "time"
		case "year":
			timeType = "year"
		case "datetime":
			timeType = "datetime"
		case "timestamp", "timestamptz", "timestampltz":
			timeType = "timestamp"
		default:
			timeType = "datetime"
		}
		return utils.IsTrueOrNot(field.Length == 0, timeType, fmt.Sprintf("%s(%d)", timeType, field.Length))
	default:
		log.DefaultLogger().Warn("handle with to default mysql type:%s", field.Type)
		return "text"
	}
}

func getTypeSuffix(field *dboperator.Field) string {
	if field.Precision > 0 && field.Scale > 0 {
		return fmt.Sprintf("(%d,%d)", field.Precision, field.Scale)
	}
	if field.Precision > 0 && field.Scale <= 0 {
		return fmt.Sprintf("(%d)", field.Precision)
	}
	if field.Precision <= 0 && field.Scale <= 0 {
		return fmt.Sprintf("(10,%d)", field.Scale)
	}
	return ""
}
