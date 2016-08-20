package main

import (
	"log"
	"os"

	"github.com/takama/daemon"
)

const (
	name        = "mysql-elasticsearch"
	description = "Replicate MySQL to Elasticsearch"
)

var stdlog, errlog *log.Logger

// Service has embedded daemon
type Service struct {
	daemon.Daemon
}

func NewService() *Service {
	daemon, err := daemon.New(name, description)
	if err != nil {
		errlog.Println("Error: ", err)
		os.Exit(1)
	}
	return &Service{daemon}
}

func init() {
	stdlog = log.New(os.Stdout, "", 0)
	errlog = log.New(os.Stderr, "", 0)
}
