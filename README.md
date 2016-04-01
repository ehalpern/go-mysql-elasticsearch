## Automatically synchronize MSYQL with ElasticSearch

_(Work in progress! Should work as advertised but still under development)_ 

This project builds on [siddontang's](https://github.com/siddontang/go-mysql-elasticsearch) very
capable foundation which:

+ Bootstraps data using [`mysqldump`](http://dev.mysql.com/doc/refman/5.7/en/mysqldump.html) and 
  then continuously synchronizes using the MySQL replication protocol
+ Provides fine grained control over the mapping between db tables and elasticsearch indexes
 
It includes the following enhancements:

+ Works with Amazon RDS. This requires a different technique for loading the initial database
  dump.
+ Works with [`mydumper`]
  (https://www.percona.com/blog/2015/11/12/logical-mysql-backup-tool-mydumper-0-9-1-now-available/) 
  which is a faster alternative to `mysqldump`.
+ Batches elasticsearch updates to significantly improve throughput when synchronizing large 
  tables.
+ Removes the need for a Makefile by moving dependencies to the vendor directory. 
 
## Install

+ Install Go and set your [GOPATH](https://golang.org/doc/code.html#GOPATH) 
    + Instructions for [Debian](https://github.com/golang/go/wiki/Ubuntu)
    + On OSX, run `brew install go`
    + For other platforms, see the [Official Documentation](https://golang.org/doc/install)
+ Run `go get github.com/ehalpern/go-mysql-elasticsearch`
+ Run `go install github.com/ehalpern/go-mysql-elasticsearch`
+ Install mydumper 0.9.1 (https://launchpad.net/ubuntu/+source/mydumper)

## How to use?

+ Create a db and tables in MySQL
+ Create configuration for connecting to MySQL and ElasticSearch and define the table to index 
  mappings. See an example config in [./etc/river.toml](./etc/river.toml) 
    + Set MySQL source in config file, see [Source](#source) below.
    + Customize MySQL and Elasticsearch mapping rule in config file, see [Rule](#rule) below.
+ Start `./bin/go-mysql-elasticsearch -config=./etc/river.toml` and enjoy it.

## Notice

+ Binlog format must be **row** ([binlog-format=row](http://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_binlog_format))
+ Binlog row image must be **full** ([binlog-row-image=full](http://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_binlog_row_image))
+ Altering the db schema requires restarting and reindexing all data
+ Each MySQL table must have a PK(primary key) which will be mapped to document _id. Multi column
  PKs are allowed and result in ids of the form "k0:k1:..." where kn is the nth component of the PK

## Source

In go-mysql-elasticsearch, you must decide which tables you want to sync into elasticsearch in the source config.

The format in config file is below:

```
[[source]]
schema = "test"
tables = ["t1", t2]

[[source]]
schema = "test_1"
tables = ["t3", t4]
```

`schema` is the database name, and `tables` includes the table need to be synced.

## Rule

By default, go-mysql-elasticsearch will use MySQL table name as the Elasticserach's index and type name, use MySQL table field name as the Elasticserach's field name.
e.g, if a table named blog, the default index and type in Elasticserach are both named blog, if the table field named title,
the default field name is also named title.

In addition, one-to-many join ( [parent-child relationship](https://www.elastic.co/guide/en/elasticsearch/guide/current/parent-child.html) in Elasticsearch ) is supported. Simply specify the field name for `parent` property.

Rule can let you change this name mapping. Rule format in config file is below:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"
parent = "parent_id"

    [[rule.fields]]
    mysql = "title"
    elastic = "my_title"
```

In the example above, we will use a new index and type both named "t" instead of default "t1", and use "my_title" instead of field name "title".

## Rule field types

In order to map a mysql column on different elasticsearch types you can define the field type as follows:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"
parent = "parent_id"

    [rule.field]
    // This will map column title to elastic search my_title
    title="my_title"

    // This will map column title to elastic search my_title and use array type
    title="my_title,list"

    // This will map column title to elastic search title and use array type
    title=",list"
```

Modifier "list" will translates a mysql string field like "a,b,c" on an elastic array type '{"a", "b", "c"}' this is specially useful if you need to use those fields on filtering on elasticsearch.

## Wildcard table

go-mysql-elasticsearch only allows you determind which table to be synced, but sometimes, if you split a big table into multi sub tables, like 1024, table_0000, table_0001, ... table_1023, it is very hard to write rules for every table.

go-mysql-elasticserach supports using wildcard table, e.g:

```
[[source]]
schema = "test"
tables = ["test_river_[0-9]{4}"]

[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"
index = "river"
type = "river"
```

"test_river_[0-9]{4}" is a wildcard table definition, which represents "test_river_0000" to "test_river_9999", at the same time, the table in the rule must be same as it.

At the above example, if you have 1024 sub tables, all tables will be synced into Elasticsearch with index "river" and type "river".

## Todo

+ Improved logging including per table statistics summaries and log file control
+ Handle schema updates in replication stream
+ Provide support for index auto-versioning so a new index can be ingested without
  interfering with the running index
+ Better documentation and examples for creating mappings
+ Add support to run as a daemon service
+ Docker container
