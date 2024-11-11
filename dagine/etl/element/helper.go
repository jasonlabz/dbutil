package element

import (
	"log/slog"
	"time"

	"github.com/bytedance/sonic"
)

func NewColumnValue(val any) ColumnValue {
	switch val.(type) {
	case nil:
		return NewNilStringColumnValue()
	case int:
		return NewBigIntColumnValueFromInt64(int64(val.(int)))
	case int8:
		return NewBigIntColumnValueFromInt64(int64(val.(int8)))
	case int16:
		return NewBigIntColumnValueFromInt64(int64(val.(int16)))
	case int32:
		return NewBigIntColumnValueFromInt64(int64(val.(int32)))
	case int64:
		return NewBigIntColumnValueFromInt64(val.(int64))
	case bool:
		return NewBoolColumnValue(val.(bool))
	case float32:
		return NewDecimalColumnValueFromFloat(float64(val.(float32)))
	case float64:
		return NewDecimalColumnValueFromFloat(val.(float64))
	case string:
		return NewStringColumnValue(val.(string))
	case []byte:
		return NewBytesColumnValue(val.([]byte))
	case []rune:
		return NewStringColumnValue(string(val.([]rune)))
	case time.Time:
		return NewTimeColumnValue(val.(time.Time))
	default:
		marshalBytes, err := sonic.Marshal(val)
		if err != nil {
			slog.Warn(err.Error())
			return NewNilStringColumnValue()
		}
		return NewStringColumnValue(string(marshalBytes))
	}
}
