package oracle

import (
	"encoding/json"

	"github.com/godror/godror"
	"github.com/jasonlabz/potato/jsonutil"
)

// Config 配置
type Config struct {
	URL      string `json:"url"`      //数据库url，包含数据库地址，数据库其他参数
	Username string `json:"username"` //用户名
	Password string `json:"password"` //密码
}

// NewConfig 创建oracle配置，如果格式不符合要求，就会报错
func NewConfig(conf *jsonutil.JSON) (c *Config, err error) {
	c = &Config{}
	err = json.Unmarshal([]byte(conf.String()), c)
	if err != nil {
		return nil, err
	}
	return
}

// FetchConnectionParams 获取oracle连接参数，url有错会报错
func (c *Config) FetchConnectionParams() (con godror.ConnectionParams, err error) {
	if con, err = godror.ParseDSN(c.URL); err != nil {
		return
	}
	con.Username = c.Username
	con.Password = godror.NewPassword(c.Password)
	return
}
