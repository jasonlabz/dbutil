package mysql

import (
	"encoding/json"

	"github.com/go-sql-driver/mysql"
	"github.com/jasonlabz/potato/jsonutil"
)

// Config mysql配置，读入的时间都需要解析即parseTime=true
type Config struct {
	URL      string `json:"url"`      //数据库url，包含数据库地址，数据库其他参数
	Username string `json:"username"` //用户名
	Password string `json:"password"` //密码
}

// NewConfig 创建mysql配置，如果格式不符合要求，就会报错
func NewConfig(conf *jsonutil.JSON) (c *Config, err error) {
	c = &Config{}
	err = json.Unmarshal([]byte(conf.String()), c)
	if err != nil {
		return nil, err
	}
	return
}

// FetchMysqlConfig 获取生成Mysql连接配置，url有错会报错
func (c *Config) FetchMysqlConfig() (conf *mysql.Config, err error) {
	if conf, err = mysql.ParseDSN(c.URL); err != nil {
		return
	}
	conf.User = c.Username
	conf.Passwd = c.Password
	conf.ParseTime = true
	return
}
