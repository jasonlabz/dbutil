// Package engine -----------------------------
// @file      : main.go
// @author    : jasonlabz
// @contact   : 1783022886@qq.com
// @time      : 2024/10/22 0:43
// -------------------------------------------
package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/jasonlabz/potato/log"
	"github.com/jasonlabz/potato/utils"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	curDir, _ := os.Getwd()
	logger := log.GetLogger(ctx)
	jobConfigs := os.Args[1:]
	if len(jobConfigs) == 0 {
		configDir := filepath.Join(curDir, "job")
		jobConfigs, _ = utils.ListDir(configDir, ".json")
		logger.Info("read job config:[%s]", configDir)
	}
	if len(jobConfigs) > 0 {
		logger.Info("readying! get job config:[%s]", strings.Join(jobConfigs, "], ["))
	}

	for _, config := range jobConfigs {
		logger.Info("start job config:[%s]", config)
	}
}
