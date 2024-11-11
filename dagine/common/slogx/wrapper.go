package slogx

import (
	"context"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jasonlabz/potato/consts"
	"github.com/jasonlabz/potato/utils"
)

var logField []string

func slogField(ctx context.Context, contextKey ...string) (fields []any) {
	for _, key := range contextKey {
		value := utils.StringValue(ctx.Value(key))
		if value == "" && key == consts.ContextTraceID {
			value = strings.ReplaceAll(uuid.New().String(), consts.SignDash, consts.EmptyString)
		}
		if value == "" {
			continue
		}
		fields = append(fields, slog.String(key, value))
	}
	return
}

func DefaultLogger() *slog.Logger {
	slog.Default().With()
	return slog.Default()
}

func GetLogger(ctx context.Context) *slog.Logger {
	return slog.Default().With(slogField(ctx, logField...)...)
}

func GormLogger(ctx context.Context) *slog.Logger {
	return slog.Default().With(slogField(ctx, logField...)...)
}

func Any(key string, val any) slog.Attr {
	return slog.Any(key, val)
}

func String(key, val string) slog.Attr {
	return slog.String(key, val)
}

func Int64(key string, val int64) slog.Attr {
	return slog.Int64(key, val)
}

func Int32(key string, val int32) slog.Attr {
	return slog.Int(key, int(val))
}

func Int(key string, val int) slog.Attr {
	return slog.Int(key, val)
}

func Float64(key string, val float64) slog.Attr {
	return slog.Float64(key, val)
}

func Float32(key string, val float32) slog.Attr {
	return slog.Float64(key, float64(val))
}
