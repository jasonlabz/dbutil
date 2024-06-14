package oracle

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jasonlabz/dbutil/dboperator"
	"github.com/jasonlabz/dbutil/log"
)

func (o OracleOperator) Trans2CommonField(dataType string) *dboperator.Field {
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
	case "char", "varchar2", "nvarchar", "nchar", "rowid", "irowid", "long", "long raw":
		// data_type: varchar、varchar(100)、etc...
		field = *dboperator.StringField
	case "date", "timestamp with time zone", "timestamp", "timestamp with local time zone":
		field = *dboperator.TimeField
		switch typeStr {
		case "timestamp with local time zone":
			field.TimeType = "timestampltz"
		case "timestamp":
			field.TimeType = "timestamp"
		case "timestamp with time zone":
			field.TimeType = "timestamptz"
		default:
			field.TimeType = "datetime"
		}
	case "clob", "nclob", "lob":
		field = *dboperator.StringField
		field.IsText = true
	case "blob", "bfile":
		field = *dboperator.BytesField
	case "smallint":
		field = *dboperator.Int16Field
	case "pls_integer", "int", "integer", "binary_integer":
		field = *dboperator.Int32Field
	case "float", "binary_float":
		field = *dboperator.Float32Field
	case "number", "numeric", "decimal", "dec":
		field = *dboperator.Float64Field
		field.IsFixedNumber = true
		if typeStr == "number" && len(extra) == 1 {
			if extra[0] <= "32" {
				field.Type = dboperator.INT32
				field.IsFixedNumber = false
			} else {
				field.Type = dboperator.INT64
				field.IsFixedNumber = false
			}
		}
	case "double precision", "double", "binary_double":
		field = *dboperator.Float64Field
	case "boolean", "bool":
		field = *dboperator.BoolField
	default:
		log.DefaultLogger().Warn("handle with default oracle type:%s", dataType)
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

func (o OracleOperator) Trans2DataType(field *dboperator.Field) string {
	switch field.Type {
	case dboperator.BYTES:
		fallthrough
	case dboperator.RUNES:
		return "BLOB"
	case dboperator.INT8, dboperator.INT16:
		if field.Precision == 0 {
			return "SMALLINT"
		} else if field.Precision == -1 {
			return "SMALLINT(*)"
		} else {
			return fmt.Sprintf("SMALLINT(%d)", field.Precision)
		}
	case dboperator.INT32, dboperator.INT64:
		if field.Precision == 0 {
			return "NUMBER"
		} else if field.Precision == -1 {
			return "NUMBER(*)"
		} else {
			return fmt.Sprintf("NUMBER(%d)", field.Precision)
		}
	case dboperator.FLOAT32:
		if field.Precision == 0 {
			return "FLOAT"
		} else if field.Precision == -1 {
			return "FLOAT(*)"
		} else {
			return fmt.Sprintf("FLOAT(%d)", field.Precision)
		}
	case dboperator.FLOAT64:
		if field.IsFixedNumber {
			return fmt.Sprintf("NUMBER%s", getTypeSuffix(field))
		}
		return "BINARY_DOUBLE"
	case dboperator.BOOL:
		return "BOOLEAN"
	case dboperator.STRING:
		if field.IsText {
			return "CLOB"
		}
		if field.Length == 0 {
			return "VARCHAR2(500)"
		} else if field.Length == -1 {
			return "VARCHAR2(*)"
		} else {
			return fmt.Sprintf("VARCHAR2(%d)", field.Length)
		}
	case dboperator.TIME:
		var timeType string
		switch field.TimeType {
		case "timestamp":
			timeType = "TIMESTAMP"
		case "timestamptz":
			return "TIMESTAMP WITH TIME ZONE"
		case "timestampltz":
			return "TIMESTAMP WITH LOCAL TIME ZONE"
		default:
			return "DATE"
		}

		if field.Length == 0 {
			return timeType
		} else if field.Length == -1 {
			return timeType + "(*)"
		} else {
			return fmt.Sprintf("timestamp(%d)", field.Length)
		}
	default:
		log.DefaultLogger().Warn("handle with to default oracle type:%s", field.Type)
		return "CLOB"
	}
}

func getTypeSuffix(field *dboperator.Field) string {
	var l, r string
	if field.Precision == -1 {
		l = "*"
	} else {
		l = fmt.Sprintf("%d", field.Precision)
	}
	if field.Scale == -1 {
		r = "*"
	} else {
		r = fmt.Sprintf("%d", field.Scale)
	}
	if field.Precision != 0 && field.Scale != 0 {
		return fmt.Sprintf("(%s,%s)", l, r)
	}
	if field.Precision != 0 && field.Scale == 0 {
		return fmt.Sprintf("(%s)", l)
	}
	return ""
}
