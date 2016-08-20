package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/takama/daemon"
)

const (
	name        = "mysql-elasticsearch"
	description = "Replicate MySQL to Elasticsearch"
)

// dependencies that are NOT required by the service, but might be used
//var dependencies = []string{"dummy.service"}

var stdlog, errlog *log.Logger

// Service has embedded daemon
type Service struct {
	daemon.Daemon
}

func New() *Service {
	daemon, err := daemon.New(name, description)
	if err != nil {
		errlog.Println("Error: ", err)
		os.Exit(1)
	}
	return &Service{daemon}
}

// Manage by daemon commands or run the daemon
func (service *Service) Manage(command string) (string, error) {

	usage := "Usage: install | remove | start | stop | status"

	switch command {
	case "install":
		return service.Install()
	case "remove":
		return service.Remove()
	case "start":
		return service.Start()
	case "stop":
		return service.Stop()
	case "status":
		return service.Status()
	default:
		return usage, nil
	}
}

func (s *Service) SetupSignalHandler() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)
}

func init() {
	stdlog = log.New(os.Stdout, "", 0)
	errlog = log.New(os.Stderr, "", 0)
}
