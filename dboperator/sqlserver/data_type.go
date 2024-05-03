package sqlserver

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jasonlabz/dbutil/core/utils"
	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/log"
)

func (s SqlServerOperator) Trans2CommonField(dataType string) *dboperator.Field {
	var field dboperator.Field
	lowerWords := strings.ToLower(dataType)
	typeStr := lowerWords
	var extra []string
	if strings.Contains(dataType, ")") {
		lIndex := strings.Index(dataType, "(")
		rIndex := strings.Index(dataType, ")")
		if lIndex != -1 {
			typeStr = dataType[:lIndex]
			extraStr := dataType[lIndex+1 : rIndex]
			extra = strings.Split(extraStr, ",")
			for i, s := range extra {
				extra[i] = strings.TrimSpace(s)
			}
		}
	}

	switch typeStr {
	case "char", "varchar", "nchar", "character", "nvarchar", "uniqueidentifier":
		// data_type: varchar、varchar(100)、etc...
		field = *dboperator.StringField
	case "date", "time", "smalldatetime", "datetime", "datetime2", "timestamp":
		field = *dboperator.TimeField
		switch typeStr {
		case "date":
			field.TimeType = "date"
		case "time":
			field.TimeType = "time"
		case "smalldatetime", "datetime", "datetime2":
			field.TimeType = "datetime"
		case "timestamp":
			field.TimeType = "timestamp"
		default:
			field.TimeType = "datetime"
		}
	case "ntext", "text", "xml":
		field = *dboperator.StringField
		field.IsText = true
	case "tinyint":
		field = *dboperator.Int8Field
	case "smallint":
		field = *dboperator.Int16Field
	case "integer", "int":
		field = *dboperator.Int32Field
	case "bigint":
		field = *dboperator.Int64Field
	case "float", "real":
		field = *dboperator.Float32Field
	case "numeric", "decimal", "money", "smallmoney":
		field = *dboperator.Float64Field
		field.IsFixedNumber = true
	case "boolean", "bool", "bit":
		field = *dboperator.BoolField
	case "binary", "varbinary", "image":
		field = *dboperator.BytesField
	default:
		log.DefaultLogger().Warn("handle with default sqlserver type:%s", dataType)
		field = *dboperator.StringField
		field.IsText = true
	}
	if len(extra) == 1 {
		field.Length, _ = strconv.Atoi(extra[0])
		field.Precision, _ = strconv.Atoi(extra[0])
	}

	if len(extra) == 2 {
		field.Scale, _ = strconv.Atoi(extra[1])
		field.Precision, _ = strconv.Atoi(extra[0])
	}
	return &field
}

func (s SqlServerOperator) Trans2DataType(field *dboperator.Field) string {
	switch field.Type {
	case dboperator.BYTES:
		fallthrough
	case dboperator.RUNES:
		return "varbinary"
	case dboperator.INT8:
		return "tinyint"
	case dboperator.INT16:
		return "smallint"
	case dboperator.INT32:
		return "int"
	case dboperator.INT64:
		return "bigint"
	case dboperator.FLOAT32:
		return "float"
	case dboperator.FLOAT64:
		if field.IsFixedNumber {
			return fmt.Sprintf("decimal%s", getTypeSuffix(field))
		}
		return "float"
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
		case "datetime":
			timeType = "datetime"
		case "timestamp", "timestamptz", "timestampltz":
			timeType = "timestamp"
		default:
			timeType = "datetime"
		}
		return timeType
	default:
		log.DefaultLogger().Warn("handle with to default sqlserver type:%s", field.Type)
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

	return ""
}
