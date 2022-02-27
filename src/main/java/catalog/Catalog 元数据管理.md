# Catalog 元数据管理

为什么需要 `catalog` ?

1. 元自动注册。`trino` 是一个很优秀的引擎，自身能够读取外部数据的元数据，自动注册并查询。 `Flink` ， `Spark` ， `clickhouse`等等这些源虽然都能跨源搜索，但是做不到`trino`那样自动注册。

   > catalog 从元数据中心读取，自动创建成表

2. 元定义共享。业务`a`，`b`  元数据是没有共享，可能出现字段理解的歧义的歧义。多个用户，重复定义元(各个任务各自指定`create xxx`，但是不是每个用户都清楚知道应该使用什么变量，比如 `kafka json` 接入是 `int`， `long`， `bigint` 精度不清)
3. 权限管理的缺失（`考虑开发时候留下对接ranger的可能`）。密码等敏感信息用户都写在`sql` 中，不安全； 生产和测试的隔离依靠人工+服务器防火墙兜底限制，存在安全风险，需要列级别的安全管理。

4. 血缘关系解析(**调研中**)。通过 `catalog`感知用户所使用的表，解析出血缘关系。



## FLink catalog 在 Sql 中的流程

![img](https://imgconvert.csdnimg.cn/aHR0cHM6Ly9tbWJpei5xcGljLmNuL21tYml6X3BuZy84QXNZQmljRWVQdTZJekJ3bzZITlliNENpYlJ3b3doaWJWUDJ0NTdobzYzREJhRm16QTBpYWJPRW5xbHVwdk5kTFRxSG1RTVQ2M2ljdmc4ZEo3cjZqdEQyZEF3LzY0MA?x-oss-process=image/format,png)

**！！！注意！！！ 上图忽略了一个细节，就是语法解析解答，会多次调用 catalog 里面的 api， 因此要确保 catalog 最低限度能正常运行，必须保证， gettable getfunction view  的正常使用。**

> TableApi 是通过 `TableApi Validator ` 对接 `catalog`， Sql 是通过`SQL Validator ` 进行对接。



## 如何开发

- 最小化实现原型需要实现

  1. 确保`gettable` 方法能返回对应的表信息。
  2. 确保`createtable`方法能保存表信息。
  3. 初始化时候调用`open`函数去获取所有元数据

  > `GenericInMemoryCatalog` 是保存在 `LinkListMap` 中，类似的，用户临时表都可以存放在`LinkListMap`中

## 元定义

将元拆分为三部分: shema , 连接信息， 所属库表。

**`Mysql`中表达如下**

- 表 schema 表达方式

```
CREATE TABLE `catalog_table_schema` (
   `table_id` int(11) NOT NULL COMMENT '表唯一标识',
   `schema_root` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0为简单类型.1为特殊类型(不能简单的表达都为1)',
   `schema_name` varchar(256) NOT NULL COMMENT 'schema名字',
   `schema_type` varchar(20480) NOT NULL COMMENT '属性类型,特殊类型会存入json方便用户二次解析,如: flink row 表达',
   `comment` text COMMENT '列属性注释',
   `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
   `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY (`table_id`,`schema_name`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8
```

- 连接信息

```
CREATE TABLE `catalog_table_conn_info` (
   `table_id` int(11) NOT NULL COMMENT '表唯一标识',
   `key` varchar(256) NOT NULL COMMENT '表连接属性名',
   `value` varchar(20480) DEFAULT NULL COMMENT '表连接属性变量',
   `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
   `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY (`table_id`,`key`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8
```

- 库表信息

```
CREATE TABLE `catalog_database` (
   `table_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '表唯一标识，自增',
   `db_name` varchar(512) DEFAULT NULL COMMENT '源属于的库',
   `table_name` varchar(512) DEFAULT NULL COMMENT '源属于的表',
   `comment` text COMMENT '源表级别注释',
   `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
   `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY (`table_id`),
   KEY `uni_table_ind` (`db_name`,`table_name`)
 ) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8
```

### schema 

`Flink` 中 `Datatype` 子类实现有 (`Flink sql`中数据类型定义)

| `Datatype` 子类    | 对应 Sql 中数据类型              |
| ------------------ | -------------------------------- |
| FieldsDataType     | row                              |
| CollectionDataType | array                            |
| AtomicDataType     | 一般非嵌套类型如：time,string 等 |
| KeyValueDataType   | map                              |



- shema 具体规则

| AtomicDataType(Flink Sql) | 元数据中表达形式样例 |
| ------------------------- | -------------------- |
| String                    | STRING               |
| Boolean                   | BOOLEAN              |
| Bytes                     | BYTES                |
| SMALLINT                  | SMALLINT             |
| Int                       | INT                  |
| Bigint                    | BIGINT               |
| Float                     | FLOAT                |
| Double                    | DOUBLE               |
| Decimal(precision,scale)  | DECIMAL(1,2)         |
| Date                      | DATE                 |
| Time                      | TIME                 |
| Timestamp()               | TIMESTAMP(3)         |

| CollectionDataType（Flink Sql） | 元数据中表达形式样例                                         |
| ------------------------------- | ------------------------------------------------------------ |
| ARRAY<STRING>                   | {"type":"ARRAY","next":{"type":"STRING"}}                    |
| ARRAY<ARRAY<STRING>>            | {"type":"ARRAY","next":{"type":"ARRAY","next":{"type":"STRING"}}} |

| KeyValueDataType(Flink Sql）  | 元数据中表达形式样例                                         |
| ----------------------------- | ------------------------------------------------------------ |
| MAP<STRING, MAP<STRING, INT>> | {"type":"MAP","key":{"type":"STRING"},"value":{"type":"MAP","key":{"type":"STRING"},"value":{"type":"INTEGER"}}} |

| FieldsDataType（Flink sql）                               | 元数据中表达形式样例                                         |
| --------------------------------------------------------- | ------------------------------------------------------------ |
| ROW<`start_time` TIME(0), `inside_row` ROW<`ins` STRING>> | {"type":"STRUCT","field":[{"type":"TIME","name":"start_time"},{"type":"STRUCT","name":"inside_row","field":[{"type":"STRING","name":"ins"}]}]} |

## 功能演示

### 直接使用源表

### 预定义 Kafka 表(Kafka 消费需要用户额外定义消费组)

### 主键&时间属性灵活定义

