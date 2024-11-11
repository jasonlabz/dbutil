package database

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"github.com/jasonlabz/potato/jsonutil"
)

// Config 数据库连接基础配置，一般用于sql.DB的配置
type Config struct {
	Pool PoolConfig `json:"pool"`
}

// NewConfig 从Json配置中获取数据库连接配置c
// err是指Json配置无法转化为数据库连接配置
func NewConfig(conf *jsonutil.JSON) (c *Config, err error) {
	c = &Config{}
	err = json.Unmarshal([]byte(conf.String()), c)
	if err != nil {
		return nil, err
	}
	return
}

// PoolConfig 数据库连接池配置
// 一般让最大打开连接数和最大空闲时连接数一致，否则会导致释放连接不及导致文件数不足
type PoolConfig struct {
	MaxOpenConns    int           `json:"maxOpenConns"`    //最大打开连接数
	MaxIdleConns    int           `json:"maxIdleConns"`    //最大空闲时连接数
	ConnMaxIdleTime time.Duration `json:"connMaxIdleTime"` //最大连接空闲时间
	ConnMaxLifetime time.Duration `json:"connMaxLifetime"` //最大连接存活时间
}

// GetMaxOpenConns 获取最大连接数，默认返回值为4
func (c *PoolConfig) GetMaxOpenConns() int {
	if c.MaxOpenConns <= 0 {
		return DefaultMaxOpenConns
	}
	return c.MaxOpenConns
}

// GetMaxIdleConns 获取空闲时最大连接数，默认返回为4
func (c *PoolConfig) GetMaxIdleConns() int {
	if c.MaxIdleConns <= 0 {
		return DefaultMaxIdleConns
	}
	return c.MaxIdleConns
}

// ConfigSetter Table的补充方法，用于设置json配置文件
type ConfigSetter interface {
	SetConfig(conf *jsonutil.JSON)
}

type BaseConfig struct {
	TrimChar bool `json:"trimChar"`
}

// BaseConfigSetter 基础表配置设置
type BaseConfigSetter struct {
	BaseConfig

	conf *jsonutil.JSON
}

// SetConfig 设置表配置
func (b *BaseConfigSetter) SetConfig(conf *jsonutil.JSON) {
	b.conf = conf
	if b.conf != nil {
		json.Unmarshal([]byte(b.conf.String()), &b.BaseConfig)
	}
}

// Config 获取表配置
func (b *BaseConfigSetter) Config() *jsonutil.JSON {
	return b.conf
}

// TrimStringChar 消除字符串 char 前后的空格
func (b *BaseConfigSetter) TrimStringChar(char string) string {
	if b.TrimChar {
		return strings.TrimSpace(char)
	}
	return char
}

// TrimByteChar 消除字节数组的 char 前后的空格
func (b *BaseConfigSetter) TrimByteChar(char []byte) []byte {
	if b.TrimChar {
		return bytes.TrimSpace(char)
	}
	return char
}
