package sqlite

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jasonlabz/dbutil/core/utils"
	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/log"
)

func (s SQLiteOperator) Trans2CommonField(dataType string) *dboperator.Field {
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
	case "date", "datetime", "timestamp":
		field = *dboperator.TimeField
		field.TimeType = "real"
	case "mediumtext", "text", "longtext":
		field = *dboperator.StringField
		field.IsText = true
	case "tinyint", "int1", "smallint", "int2":
		field = *dboperator.Int32Field
	case "mediumint", "int", "integer", "int3", "int4":
		field = *dboperator.Int64Field
	case "float", "double", "real":
		field = *dboperator.Float64Field
	case "numeric", "decimal", "number":
		field = *dboperator.Float64Field
		field.IsFixedNumber = true
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

func (s SQLiteOperator) Trans2DataType(field *dboperator.Field) string {
	switch field.Type {
	case dboperator.RUNES, dboperator.BYTES:
		return "BLOB"
	case dboperator.INT8, dboperator.INT16, dboperator.INT32, dboperator.INT64:
		return utils.IsTrueOrNot(field.Precision <= 0, "INTEGER", fmt.Sprintf("tinyint(%d)", field.Precision))
	case dboperator.FLOAT32:
		return fmt.Sprintf("FLOAT%s", getTypeSuffix(field))
	case dboperator.FLOAT64:
		if field.IsFixedNumber {
			return fmt.Sprintf("DECIMAL%s", getTypeSuffix(field))
		}
		return "REAL"
	case dboperator.BOOL:
		return "BOOLEAN"
	case dboperator.STRING:
		if field.IsText {
			return "TEXT"
		}
		return utils.IsTrueOrNot(field.Length == 0, "VARCHAR", fmt.Sprintf("VARCHAR(%d)", field.Length))
	case dboperator.TIME:
		var timeType string
		switch field.TimeType {
		case "real_date":
			timeType = "DATE"
		case "real_datetime":
			timeType = "DATETIME"
		default:
			timeType = "VARCHAR(50)"
		}
		return timeType
	default:
		log.DefaultLogger().Warn("handle with to default sqlite type:%s", field.Type)
		return "TEXT"
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
