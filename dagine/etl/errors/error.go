// Package errors -----------------------------
// @file      : error.go
// @author    : jasonlabz
// @contact   : 1783022886@qq.com
// @time      : 2024/10/26 0:14
// -------------------------------------------
package errors

import "github.com/jasonlabz/potato/errors"

var (
	ErrNoPlugin   = errors.New(300000000, "plugin not found")
	ErrInitPlugin = errors.New(300000001, "plugin init error")
)
