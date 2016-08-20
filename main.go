package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/ehalpern/go-mysql-elasticsearch/river"
	"github.com/juju/errors"
	"fmt"
)

var configFile = flag.String("config", "./etc/river.toml", "replication config file")
var serviceOp = flag.String("service", "", "install|remove|[re]start|stop|status")
var dbHost = flag.String("db_host", "", "DB host and port")
var dbUser = flag.String("db_user", "", "DB user")
var dbPass = flag.String("db_pass", "", "DB password")
var dbSlaveId = flag.Int("db_slave_id", 0, "MySQL slave id")
var esHost = flag.String("es_host", "", "Elasticsearch host and port")
var esMaxActions = flag.Int("es_max_actions", 0, "maximum elasticsearch bulk update size")
var dataDir = flag.String("data_dir", "", "path do store data")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)

	flag.Parse()

	if *serviceOp != "" {
		s := NewService()
		var status string
		var err error

		switch *serviceOp {
		case "install":
			status, err = s.Install("-config", *configFile)
		case "remove":
			status, err = s.Remove()
		case "start":
			status, err = s.Start()
		case "stop":
			status, err = s.Stop()
		case "status":
			status, err = s.Status()
		default:
			flag.Usage()
			err = errors.Errorf("unrecognized -service option " + *serviceOp)
		}
		if err != nil {
			errlog.Println("Error: ", err)
			os.Exit(1)
		}
		fmt.Println(status)
		os.Exit(1)
	}

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
	if *dbSlaveId > 0 {
		cfg.DbSlaveID = uint32(*dbSlaveId)
	}
	if len(*esHost) > 0 {
		cfg.EsHost = *esHost
	}
	if len(*dataDir) > 0 {
		cfg.DataDir = *dataDir
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

	<-interrupt
	r.Close()
}
