# dbutil

数据库表结构迁移工具
简介

本工具用于简化数据库表结构的迁移操作。它可以自动比较源数据库和目标数据库的表结构差异，并生成相应的迁移脚本。用户只需执行生成的脚本即可完成表结构的迁移。

- 功能

比较源数据库和目标数据库的表结构差异
生成迁移脚本
支持多种数据库类型，包括：mysql、oracle、postgres、sqlserver等
支持迁移表结构的各种元素，包括：表名、字段名、字段类型、约束等
- 安装

```Bash
go install github.com/jasonlabz/dbutil@master
```

- 使用

配置源数据库和目标数据库的信息。

示例

```bash
dbutil -c '{
  "source": {
    "db_name": "source",
    "dsn": "user:password@tcp(host:port)/typecho?charset=utf8mb4&parseTime=True&loc=Local&timeout=30s",
    "db_type": "mysql"
  },
  "target": {
    "db_name": "target",
    "dsn": "user=postgres password=******* host=192.168.3.30 port=5432 dbname=postgres sslmode=disable TimeZone=Asia/Shanghai",
    "db_type": "postgres"
  },
  "sourceSchema": "lg_server",
  "targetSchema": "public",
  "tableList": []
}' -p "/ddl_save_path"
```
注释： 
`-c` 指定数据库的相关信息，`-p`为可选，指定生成的ddl语句保存位置 \
`source`配置源端数据库连接信息，数据源类型支持mysql、oracle、postgres、sqlserver； \
`target`配置目的端数据库连接信息，数据源类型支持mysql、oracle、postgres、sqlserver； \
`sourceSchema`指定源端schema名（如oracle为owner等），需要同步哪个schema下的库表； \
`targetSchema`指定目的端schema名（如oracle为owner等），需要在哪个schema下建立新的库表。\
`tableList`可以指定只迁移部分表列表 \

```go
// DatabaseDsnMap 关系型数据库类型  username、password、address、port、dbname
var DatabaseDsnMap = map[DBType]string{
	DBTypeOracle:    "%s/%s@%s:%d/%s",
	DBTypeMySQL:     "%s:%s@tcp(%s:%d)/%s?parseTime=True&loc=Local",
	DBTypePostgres:  "user=%s password=%s host=%s port=%d dbname=%s sslmode=disable TimeZone=Asia/Shanghai",
	DBTypeSqlserver: "user id=%s;password=%s;server=%s;port=%d;database=%s",
}
```
注意

在使用本工具之前，请务必备份数据库。
本工具仅用于表结构的迁移，不迁移表数据。
如果您需要迁移表数据，请使用其他工具。
联系方式

如果您在使用本工具时遇到任何问题，请随时联系。

邮箱： [1783022886@qq.com]
