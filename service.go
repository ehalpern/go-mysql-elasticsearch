package main

import (
	"log"
	"os"

	"github.com/takama/daemon"
	"github.com/ehalpern/go-mysql-elasticsearch/river"
)

var stdlog, errlog *log.Logger

// Service has embedded daemon
type Service struct {
	daemon.Daemon
}

func NewService() (*Service, error) {
	daemon, err := daemon.New(river.ServiceName, river.ServiceDesc)
	if err != nil {
		return nil, err
	}
	return &Service{daemon}, nil
}

func init() {
	stdlog = log.New(os.Stdout, "", 0)
	errlog = log.New(os.Stderr, "", 0)
}
