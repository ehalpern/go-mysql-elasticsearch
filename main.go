package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/ehalpern/go-mysql-elasticsearch/river"
	"github.com/juju/errors"
)

var configFile = flag.String("config", "./etc/river.toml", "replication config file")
var dbHost = flag.String("my_addr", "", "DB host and port")
var dbUser = flag.String("my_user", "", "DB user")
var dbPass = flag.String("my_pass", "", "DB password")
var dbFlavor = flag.String("flavor", "", "DB flavor [mysql | mariadb]")
var dbServerId = flag.Int("server_id", 0, "MySQL server id, as a pseudo slave")
var esHost = flag.String("es_addr", "", "Elasticsearch host and port")
var esMaxActions = flag.Int("es_max_actions", 0, "maximum size of an elasticsearch bulk update")
var dataDir = flag.String("data_dir", "", "path do store data")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg, err := river.NewConfigWithFile(*configFile)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if len(*dbHost) > 0 {
		cfg.DbHost = *dbHost
	}

	if len(*dbUser) > 0 {
		cfg.DbUser = *dbUser
	}

	if len(*dbPass) > 0 {
		cfg.DbPassword = *dbPass
	}

	if *dbServerId > 0 {
		cfg.DbSlaveID = uint32(*dbServerId)
	}

	if len(*esHost) > 0 {
		cfg.EsHost = *esHost
	}

	if len(*dataDir) > 0 {
		cfg.DataDir = *dataDir
	}

	if len(*dbFlavor) > 0 {
		cfg.DbFlavor = *dbFlavor
	}

	if len(*dumpProg) > 0 {
		cfg.DumpExec = *dumpProg
	}

	if *esMaxActions > 0 {
		cfg.EsMaxActions = *esMaxActions
	}

	r, err := river.NewRiver(cfg)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if err = r.Run(); err != nil {
		println(errors.ErrorStack(err))
	}

	<-sc
	r.Close()
}
