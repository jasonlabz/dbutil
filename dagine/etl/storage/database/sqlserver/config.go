package sqlserver

import (
	"encoding/json"

	"github.com/denisenkom/go-mssqldb/msdsn"
	"github.com/jasonlabz/potato/jsonutil"
)

// Config mssql配置
type Config struct {
	URL      string `json:"url"`      //数据库url，包含数据库地址，数据库其他参数
	Username string `json:"username"` //用户名
	Password string `json:"password"` //密码
}

// NewConfig 创建mssql配置，如果格式不符合要求，就会报错
func NewConfig(conf *jsonutil.JSON) (c *Config, err error) {
	c = &Config{}
	err = json.Unmarshal([]byte(conf.String()), c)
	if err != nil {
		return nil, err
	}
	return
}

func (c *Config) fetchMssqlConfig() (conf msdsn.Config, err error) {
	conf, _, err = msdsn.Parse(c.URL)
	if err != nil {
		return
	}
	conf.User = c.Username
	conf.Password = c.Password
	return
}

// FormatDSN 获取数据库连接信息
func (c *Config) FormatDSN() (dsn string, err error) {
	var conf msdsn.Config
	conf, err = c.fetchMssqlConfig()
	if err != nil {
		return
	}
	dsn = conf.URL().String()
	return
}
