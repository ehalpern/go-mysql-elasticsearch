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

func NewService() (*Service, error) {
	daemon, err := daemon.New(name, name)
	if err != nil {
		return nil, err
	}
	return &Service{daemon}
}

func init() {
	stdlog = log.New(os.Stdout, "", 0)
	errlog = log.New(os.Stderr, "", 0)
}
