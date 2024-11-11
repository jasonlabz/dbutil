package xlsx

import (
	"encoding/json"
	"fmt"

	"github.com/jasonlabz/potato/jodatime"
	"github.com/jasonlabz/potato/jsonutil"
	"github.com/xuri/excelize/v2"

	"github.com/jasonlabz/dagine/etl/element"
)

// InConfig 输入xlsx配置
type InConfig struct {
	Columns    []Column `json:"column"`     //列信息数组
	Sheet      string   `json:"sheet"`      //表格名
	NullFormat string   `json:"nullFormat"` //null文本
	StartRow   int      `json:"startRow"`   //开始读取行数，从第1行开始
}

// NewInConfig 新建以json配置conf的输入xlsx配置
func NewInConfig(conf *jsonutil.JSON) (c *InConfig, err error) {
	c = &InConfig{}
	err = json.Unmarshal([]byte(conf.String()), c)
	if err != nil {
		return nil, err
	}

	if c.Sheet == "" {
		return nil, fmt.Errorf("sheet should not be empty")
	}

	for _, v := range c.Columns {
		if err = v.validate(); err != nil {
			return nil, err
		}
	}
	return
}

func (c *InConfig) startRow() int {
	if c.StartRow == 0 {
		return 1
	}
	return c.StartRow
}

// OutConfig 输出xlsx配置
type OutConfig struct {
	Columns    []Column `json:"column"`     //列信息数组
	Sheets     []string `json:"sheets"`     //表格名
	NullFormat string   `json:"nullFormat"` //null文本
	HasHeader  bool     `json:"hasHeader"`  // 是否有列头
	Header     []string `json:"header"`     // 列头
	SheetRow   int      `json:"sheetRow"`   // sheet最大的行数
}

// NewOutConfig 新建以json配置conf的输出xlsx配置
func NewOutConfig(conf *jsonutil.JSON) (c *OutConfig, err error) {
	c = &OutConfig{}
	err = json.Unmarshal([]byte(conf.String()), c)
	if err != nil {
		return nil, err
	}
	if len(c.Sheets) == 0 {
		return nil, fmt.Errorf("sheets should not be empty")
	}

	if c.SheetRow > excelize.TotalRows || c.SheetRow < 0 {
		return nil, fmt.Errorf("sheetRow should be not less than %v and positive", excelize.TotalRows)
	}

	for _, v := range c.Columns {
		if err = v.validate(); err != nil {
			return nil, err
		}
	}
	return
}

func (c *OutConfig) sheetRow() int {
	if c.SheetRow == 0 {
		return excelize.TotalRows
	}
	return c.SheetRow
}

// Column 列信息
type Column struct {
	Index    string `json:"index"`  //列索引，A,B,C....AA.....
	Type     string `json:"type"`   //类型 类型 bool bigInt decimal string time
	Format   string `json:"format"` //joda时间格式
	indexNum int
	goLayout string
}

// validate 校验
func (c *Column) validate() (err error) {
	switch element.ColumnType(c.Type) {
	case element.TypeBool, element.TypeBigInt,
		element.TypeDecimal, element.TypeString:
	case element.TypeTime:
		if c.Format == "" {
			return fmt.Errorf("type %v format %v is empty", c.Type, c.Format)
		}
	default:
		return fmt.Errorf("type %v is not valid", c.Type)
	}

	if _, err = excelize.ColumnNameToNumber(c.Index); err != nil {
		return fmt.Errorf("index %v err: %v", c.Type, err)
	}
	return
}

// index 列索引
func (c *Column) index() (i int) {
	if c.indexNum > 0 {
		return c.indexNum - 1
	}
	c.indexNum, _ = excelize.ColumnNameToNumber(c.Index)
	return c.indexNum - 1
}

// layout go时间格式
func (c *Column) layout() string {
	if c.goLayout != "" {
		return c.goLayout
	}
	c.goLayout = jodatime.GetLayout(c.Format)
	return c.goLayout
}
