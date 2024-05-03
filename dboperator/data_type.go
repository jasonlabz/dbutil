package dboperator

type FieldType string

const (
	BYTES   FieldType = "[]byte"
	RUNES   FieldType = "[]rune"
	INT8    FieldType = "int8"
	INT16   FieldType = "int16"
	INT32   FieldType = "int32"
	INT64   FieldType = "int64"
	FLOAT32 FieldType = "float32"
	FLOAT64 FieldType = "float64"
	BOOL    FieldType = "bool"
	STRING  FieldType = "string"
	TIME    FieldType = "time"
)

type Field struct {
	Type          FieldType
	ColumnName    string
	IsText        bool   // 区分字符串和文本
	IsFixedNumber bool   // 区分浮点数和定点数
	TimeType      string // 区分时间类型 date|datetime|year|time|timetz|timestamp|timestamptz
	StringValue   string
	Int64Value    int64
	Int32Value    int32
	TimeValue     string
	Float32Value  float32
	Float64Value  float64
	Length        int // 文本长度
	Scale         int // 小数点
	Precision     int // 精度
}

var (
	BytesField = &Field{
		Type: BYTES,
	}

	TimeField = &Field{
		Type: TIME,
	}

	BoolField = &Field{
		Type: BOOL,
	}

	StringField = &Field{
		Type: STRING,
	}

	Int8Field = &Field{
		Type: INT8,
	}

	Int16Field = &Field{
		Type: INT16,
	}

	Int32Field = &Field{
		Type: INT32,
	}

	Int64Field = &Field{
		Type: INT64,
	}

	Float32Field = &Field{
		Type: FLOAT32,
	}

	Float64Field = &Field{
		Type: FLOAT64,
	}
)

type ITransfer interface {
	Trans2CommonField(dataType string) *Field
	Trans2DataType(field *Field) string
}
