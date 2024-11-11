// Package engine -----------------------------
// @file      : engine.go
// @author    : jasonlabz
// @contact   : 1783022886@qq.com
// @time      : 2024/10/23 23:25
// -------------------------------------------
package etl

import (
	"context"
	"github.com/jasonlabz/dagine/etl/errors"
	"github.com/jasonlabz/dagine/operator"
	"sync"
)

// HandlersChain defines a HandlerFunc slice.
type HandlersChain []operator.Transformer

//// Last returns the last handler in the chain. i.e. the last handler is the main one.
//func (c HandlersChain) Last() operator.Plugin {
//	if length := len(c); length > 0 {
//		return c[length-1]
//	}
//	return nil
//}

type Task struct {
	reader      operator.Reader
	writer      operator.Writer
	transformer HandlersChain
	once        sync.Once
	pool        sync.Pool
}

func (e *Task) run(ctx context.Context) error {
	if e.reader == nil ||
		e.writer == nil {
		return errors.ErrNoPlugin
	}

	var err error
	// 插件初始化
	e.once.Do(func() {
		err = e.reader.Init(ctx)
		if err != nil {
			err = errors.ErrInitPlugin.WithErr(err)
			return
		}

		err = e.writer.Init(ctx)
		if err != nil {
			err = errors.ErrInitPlugin.WithErr(err)
			return
		}
	})
	if err != nil {
		return err
	}

	e.reader.Run(ctx)
	return nil
}

type Engine struct {
	worker chan int
}
