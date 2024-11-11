// Package operator -----------------------------
// @file      : plugin.go
// @author    : jasonlabz
// @contact   : 1783022886@qq.com
// @time      : 2024/10/10 0:29
// -------------------------------------------
package operator

import (
	"context"

	"github.com/jasonlabz/dagine/etl/base"
	"github.com/jasonlabz/dagine/etl/element"
)

type Reader interface {
	Run(ctx context.Context) (outData []*element.Record, outFields []base.Field, err error)
	Init(ctx context.Context) (err error)
	Release(ctx context.Context) (err error)
}

type Writer interface {
	Run(ctx context.Context, inData []*element.Record, inFields ...base.Field) (err error)
	Init(ctx context.Context) (err error)
	Release(ctx context.Context) (err error)
}

type Transformer interface {
	ProcessFunc(ctx context.Context, inData []*element.Record, inFields ...base.Field) (outData []*element.Record, outFields []base.Field, err error)
	InitFunc(ctx context.Context) (err error)
	DestroyFunc(ctx context.Context) (err error)
}
