package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/ehalpern/mysql2es/config"
	"github.com/ehalpern/mysql2es/river"
	"github.com/juju/errors"
)

var options = struct {
	help         *bool
	service      *string
	config       *string
	dataDir      *string
	dbHost       *string
	dbUser       *string
	dbPassword   *string
	dbSlaveID    *int
	esHost       *string
	esMaxActions *int
	reuseDump    *string
}{
	flag.Bool("help", false, "show help"),
	flag.String("service", "", "install|remove|[re]start|stop|status"),
	flag.String("config", config.Default.ConfigFile, "config file"),
	flag.String("data_dir", "", fmt.Sprintf("data directory (%s)", config.Default.DataDir)),
	flag.String("db_host", "", fmt.Sprintf("DB host and port (%s)", config.Default.DbHost)),
	flag.String("db_user", "", fmt.Sprintf("DB user (%s)", config.Default.DbUser)),
	flag.String("db_pass", "", fmt.Sprintf("DB password (%s)", config.Default.DbPassword)),
	flag.Int("db_slave_id", 1001, fmt.Sprintf("MySQL slave id (%s)", config.Default.DbSlaveID)),
	flag.String("es_host", "", fmt.Sprintf("Elasticsearch host and port (%s)", config.Default.EsHost)),
	flag.Int("es_max_actions", 1, fmt.Sprintf("maximum elasticsearch bulk update size (%s)", config.Default.EsMaxActions)),
	flag.String("use_dump", "", "use dump stored in this directory rather than generating new dump"),
}

func main() {
	var status string
	var err error

	flag.Parse()
	if *options.help {
		flag.Usage()
		os.Exit(0)
	}
	// ensure absolute path
	if *options.config, err = filepath.Abs(*options.config); err != nil {
		panic(err.Error())
	}

	if *options.service != "" {
		status, err = invokeService(*options.service)
	} else {
		err = runNormally()
	}
	if err != nil {
		errlog.Println("Error: ", err)
		os.Exit(1)
	} else {
		fmt.Println(status)
		os.Exit(0)
	}
}

func invokeService(cmd string) (string, error) {
	configFile, err := filepath.Abs(*options.config)
	if err != nil {
		return "", err
	}
	s, err := NewService()
	if err != nil {
		return "", err
	}

	switch cmd {
	case "install":
		return s.Install("-config", configFile)
	case "remove":
		return s.Remove()
	case "start":
		return s.Start()
	case "stop":
		return s.Stop()
	case "status":
		return s.Status()
	default:
		return "", errors.Errorf("unrecognized -service option " + cmd)
	}
}

func runNormally() error {
	runtime.GOMAXPROCS(runtime.NumCPU())
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)

	cfg, err := config.NewConfigWithFile(*options.config)
	if err != nil {
		return err
	}

	if len(*options.dbHost) > 0 {
		cfg.DbHost = *options.dbHost
	}
	if len(*options.dbUser) > 0 {
		cfg.DbUser = *options.dbUser
	}
	if len(*options.dbPassword) > 0 {
		cfg.DbPassword = *options.dbPassword
	}
	if *options.dbSlaveID > 0 {
		cfg.DbSlaveID = uint32(*options.dbSlaveID)
	}
	if len(*options.esHost) > 0 {
		cfg.EsHost = *options.esHost
	}
	if len(*options.dataDir) > 0 {
		cfg.DataDir = *options.dataDir
	}
	if *options.esMaxActions > 0 {
		cfg.EsMaxActions = *options.esMaxActions
	}

	river, err := river.NewRiver(cfg)
	if err != nil {
		return err
	}

	if err = river.Run(); err != nil {
		return err
	}

	<-interrupt
	river.Close()
	return nil
}
