package postgresql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jasonlabz/dbutil/core/utils"
	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/log"
)

func (p PGOperator) Trans2CommonField(dataType string) *dboperator.Field {
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
	case "char", "bpchar", "varchar", "character varying", "character":
		// data_type: varchar、varchar(100)、etc...
		field = *dboperator.StringField
	case "date", "time", "timetz", "time without time zone", "time with time zone", "timestamp with time zone", "timestamp without time zone", "timestamp", "timestamptz":
		field = *dboperator.TimeField
		switch typeStr {
		case "date":
			field.TimeType = "date"
		case "time", "time without time zone":
			field.TimeType = "time"
		case "time with time zone", "timetz":
			field.TimeType = "timetz"
		case "timestamp", "timestamp without time zone":
			field.TimeType = "timestamp"
		case "timestamp with time zone", "timestamptz":
			field.TimeType = "timestamptz"
		default:
			field.TimeType = "timestamp"
		}
	case "text":
		field = *dboperator.StringField
		field.IsText = true
	case "bytea":
		field = *dboperator.BytesField
	case "smallint", "int2":
		field = *dboperator.Int16Field
	case "int", "integer", "int4", "serial":
		field = *dboperator.Int32Field
	case "bigint", "int8", "bigserial":
		field = *dboperator.Int64Field
	case "float", "real", "float4":
		field = *dboperator.Float32Field
	case "numeric", "decimal", "dec":
		field = *dboperator.Float64Field
		field.IsFixedNumber = true
	case "double precision", "float8":
		field = *dboperator.Float64Field
	case "boolean", "bool":
		field = *dboperator.BoolField
	default:
		log.DefaultLogger().Warn("handle with default postgresql type:%s", dataType)
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

func (p PGOperator) Trans2DataType(field *dboperator.Field) string {
	switch field.Type {
	case dboperator.BYTES:
		fallthrough
	case dboperator.RUNES:
		return "bytea"
	case dboperator.INT8:
		fallthrough
	case dboperator.INT16:
		return "smallint"
	case dboperator.INT32:
		return "integer"
	case dboperator.INT64:
		return "bigint"
	case dboperator.FLOAT32:
		return "float"
	case dboperator.FLOAT64:
		if field.IsFixedNumber {
			return fmt.Sprintf("decimal%s", getTypeSuffix(field))
		}
		return "double precision"
	case dboperator.BOOL:
		return "boolean"
	case dboperator.STRING:
		if field.IsText {
			return "text"
		}
		return utils.IsTrueOrNot(field.Length <= 0, "varchar", fmt.Sprintf("varchar(%d)", field.Length))
	case dboperator.TIME:
		var timeType string
		switch field.TimeType {
		case "date":
			return "date"
		case "time":
			timeType = "time"
		case "timetz":
			timeType = "timetz"
		case "timestamp":
			timeType = "timestamp"
		case "timestamptz", "timestampltz":
			timeType = "timestamptz"
		default:
			timeType = "timestamp"
		}
		return utils.IsTrueOrNot(field.Length == 0, timeType, fmt.Sprintf("%s(%d)", timeType, field.Length))
	default:
		log.DefaultLogger().Warn("handle with to default postgresql type:%s", field.Type)
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
