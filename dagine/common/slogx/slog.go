package slogx

import (
	"github.com/jasonlabz/potato/consts"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/jasonlabz/potato/configx"
	"github.com/jasonlabz/potato/configx/file"
	"github.com/jasonlabz/potato/syncer"
	"github.com/jasonlabz/potato/times"
	"github.com/jasonlabz/potato/utils"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	logLevel                 = slog.LevelInfo
	defaultLoggerConfigName  = "default_slog_config"
	defaultLoggerConfigPaths = []string{
		"./conf/logger.yaml",
		"./conf/app.yaml",
		"./conf/application.yaml",
		"./conf/zap.yaml",
		"./logger.yaml",
		"./app.yaml",
		"./application.yaml",
		"./zap.yaml",
	}
)

func init() {
	InitLogger()
}

type Options struct {
	writeFile  bool
	logFormat  string
	name       string   // 应用名
	configPath string   // 日志配置文件
	keyList    []string // 自定义context中需要打印的Field字段
	logLevel   string   // 日志级别
	basePath   string   // 日志目录
	fileName   string   // 日志w文件
	maxSize    int      // 文件大小限制,单位MB
	maxAge     int      // 日志文件保留天数
	maxBackups int      // 最大保留日志文件数量
	compress   bool     // Compress确定是否应该使用gzip压缩已旋转的日志文件。默认值是不执行压缩。
}

type Option func(o *Options)

func WithName(name string) Option {
	return func(o *Options) {
		o.name = name
	}
}
func WithLevel(level string) Option {
	return func(o *Options) {
		o.logLevel = level
	}
}

func WithBasePath(basePath string) Option {
	return func(o *Options) {
		o.basePath = basePath
	}
}

func WithFileName(fileName string) Option {
	return func(o *Options) {
		o.fileName = fileName
	}
}

func WithLogField(key string) Option {
	return func(o *Options) {
		o.keyList = append(o.keyList, key)
	}
}

func WithLogFields(keys ...string) Option {
	return func(o *Options) {
		o.keyList = append(o.keyList, keys...)
	}
}

func WithConfigPath(path string) Option {
	return func(o *Options) {
		o.configPath = path
	}
}

func InitLogger(opts ...Option) {
	options := &Options{}

	for _, opt := range opts {
		opt(options)
	}
	// 读取zap配置文件
	var configLoad bool
	if !configLoad && options.configPath != "" && utils.IsExist(options.configPath) {
		provider, err := file.NewConfigProvider(options.configPath)
		if err != nil {
			log.Printf("init logger {%s} err: %v", options.configPath, err)
			configLoad = false
		} else {
			configx.AddProviders(defaultLoggerConfigName, provider)
			configLoad = true
		}
	}

	for _, confPath := range defaultLoggerConfigPaths {
		if configLoad {
			break
		}
		if utils.IsExist(confPath) {
			provider, err := file.NewConfigProvider(confPath)
			if err != nil {
				log.Printf("init logger {%s} err: %v", confPath, err)
				continue
			}
			configx.AddProviders(defaultLoggerConfigName, provider)
			configLoad = true
		}
	}

	if !configLoad {
		log.Printf("log init by default config")
	}
	// 加载配置
	loadConf(options)

	levelConfig := configx.GetString(defaultLoggerConfigName, "log.log_level")

	// 优先程序配置
	if options.logLevel != "" {
		levelConfig = options.logLevel
	}
	switch levelConfig {
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "debug":
		logLevel = slog.LevelDebug
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	//lowLevel文件WriteSyncer
	lowLevelFileWriteSyncer := getLowLevelWriterSyncer()

	var handler slog.Handler
	writeSyncer := func() syncer.WriteSyncer {
		if options.writeFile {
			return syncer.NewMultiWriteSyncer(lowLevelFileWriteSyncer, syncer.AddSync(os.Stdout))
		}
		return syncer.AddSync(os.Stdout)
	}()
	sOption := &slog.HandlerOptions{
		AddSource: true,
		Level:     logLevel,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				a.Value = slog.StringValue(time.Now().Format(times.MilliTimeFormat))
			}
			return a
		},
	}
	if options.logFormat == "json" {
		handler = slog.NewJSONHandler(writeSyncer, sOption)
	} else {
		handler = slog.NewTextHandler(writeSyncer, sOption)
	}

	slog.SetDefault(slog.New(handler))
	return
}

// core 三个参数之  日志输出路径
func getLowLevelWriterSyncer() syncer.WriteSyncer {
	filename := func() string {
		getString := configx.GetString(defaultLoggerConfigName, "log.log_file_conf.log_file_path")
		if getString == "" {
			getString = "./log/app.log"
		}
		return getString
	}()
	maxSize := func() int {
		geInt := configx.GetInt(defaultLoggerConfigName, "log.log_file_conf.max_size")
		if geInt == 0 {
			return 300
		}
		return geInt
	}()
	maxAge := func() int {
		geInt := configx.GetInt(defaultLoggerConfigName, "log.log_file_conf.max_age")
		if geInt == 0 {
			return 10
		}
		return geInt
	}()
	maxBackups := func() int {
		geInt := configx.GetInt(defaultLoggerConfigName, "log.log_file_conf.max_backups")
		if geInt == 0 {
			return 15
		}
		return geInt
	}()
	compress := configx.GetBool(defaultLoggerConfigName, "log.log_file_conf.compress")

	//引入第三方库 Lumberjack 加入日志切割功能
	infoLumberIO := &lumberjack.Logger{
		Filename:   filename,   //日志文件存放目录，如果文件夹不存在会自动创建
		MaxSize:    maxSize,    //文件大小限制,单位MB
		MaxBackups: maxBackups, //最大保留日志文件数量
		MaxAge:     maxAge,     //日志文件保留天数
		Compress:   compress,   //Compress确定是否应该使用gzip压缩已旋转的日志文件。默认值是不执行压缩。
	}
	return syncer.AddSync(infoLumberIO)
}

func loadConf(options *Options) {
	defaultOptions := Options{
		writeFile:  false,
		logFormat:  "console",
		configPath: "./conf/logger.yaml",
		keyList:    []string{consts.ContextLOGID, consts.ContextTraceID, consts.ContextUserID},
		logLevel:   "info",
		basePath:   "./log",
		fileName:   "app.log",
		maxSize:    15,
		maxAge:     7,
		maxBackups: 30,
		compress:   false,
	}

	level := configx.GetString(defaultLoggerConfigName, "log.log_level")
	options.logLevel = utils.IsTrueOrNot(options.logLevel == "",
		utils.IsTrueOrNot(level == "", defaultOptions.logLevel, level), options.logLevel)

	logFormat := configx.GetString(defaultLoggerConfigName, "log.format")
	options.logFormat = utils.IsTrueOrNot(options.logFormat == "",
		utils.IsTrueOrNot(logFormat == "", defaultOptions.logFormat, logFormat), options.logFormat)

	writeFile := configx.GetBool(defaultLoggerConfigName, "log.write_file")
	options.writeFile = utils.IsTrueOrNot(!options.writeFile,
		utils.IsTrueOrNot(!writeFile, defaultOptions.writeFile, writeFile), options.writeFile)

	basePath := configx.GetString(defaultLoggerConfigName, "log.log_file_conf.base_path")
	options.basePath = utils.IsTrueOrNot(options.basePath == "",
		utils.IsTrueOrNot(basePath == "", defaultOptions.basePath, basePath), options.basePath)

	fileName := configx.GetString(defaultLoggerConfigName, "log.log_file_conf.file_name")
	options.fileName = utils.IsTrueOrNot(options.fileName == "",
		utils.IsTrueOrNot(fileName == "", defaultOptions.fileName, fileName), options.fileName)

	maxSize := configx.GetInt(defaultLoggerConfigName, "log.log_file_conf.max_size")
	options.maxSize = utils.IsTrueOrNot(options.maxSize == 0,
		utils.IsTrueOrNot(maxSize == 0, defaultOptions.maxSize, maxSize), options.maxSize)

	maxAge := configx.GetInt(defaultLoggerConfigName, "log.log_file_conf.max_age")
	options.maxAge = utils.IsTrueOrNot(options.maxAge == 0,
		utils.IsTrueOrNot(maxAge == 0, defaultOptions.maxAge, maxAge), options.maxAge)

	maxBackups := configx.GetInt(defaultLoggerConfigName, "log.log_file_conf.max_backups")
	options.maxBackups = utils.IsTrueOrNot(options.maxBackups == 0,
		utils.IsTrueOrNot(maxBackups == 0, defaultOptions.maxBackups, maxBackups), options.maxBackups)

	compress := configx.GetBool(defaultLoggerConfigName, "log.log_file_conf.compress")
	options.compress = utils.IsTrueOrNot(!options.compress,
		utils.IsTrueOrNot(!compress, defaultOptions.compress, compress), options.compress)

	if len(options.keyList) == 0 {
		options.keyList = defaultOptions.keyList
	}

	if options.name != "" {
		options.basePath = filepath.Join(options.basePath, options.name)
	}

	logField = options.keyList
}
