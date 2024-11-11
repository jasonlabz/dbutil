package bootstrap

import (
	"github.com/jasonlabz/potato/configx"
	"github.com/jasonlabz/potato/cryptox"
	"github.com/jasonlabz/potato/cryptox/aes"
	"github.com/jasonlabz/potato/cryptox/des"
	"github.com/jasonlabz/potato/gormx"
	_ "github.com/jasonlabz/potato/log"
	"github.com/jasonlabz/potato/utils"
)

func init() {
	// 初始化加解秘钥
	initCrypto()
	// 初始化DB
	initDB()
}

func initCrypto() {
	cryptoConfigs := configx.GetConfig().Crypto
	for _, conf := range cryptoConfigs {
		if conf.Key == "" {
			continue
		}
		switch conf.Type {
		case cryptox.CryptoTypeAES:
			aes.SetAESCrypto(aes.NewAESCrypto([]byte(conf.Key)))
		case cryptox.CryptoTypeDES:
			des.SetDESCrypto(des.NewDESCrypto([]byte(conf.Key)))
		}
	}
}

func initDB() {
	conf := configx.GetConfig()
	if conf.Database == nil {
		panic("no db config")
	}
	gormConfig := &gormx.Config{}
	err := utils.CopyStruct(conf.Database, gormConfig)
	if err != nil {
		panic(err)
	}
	gormConfig.DBName = gormx.DefaultDBNameMaster
	err = gormx.InitConfig(gormConfig)
	if err != nil {
		panic(err)
	}
	//impl.SetGormDB(gormx.GetDBWithPanic(gormConfig.DBName))
}
