package main

import (
	"flag"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func main() {
	// Parse empty flags to suppress warnings from the snapshotter which uses
	// glog
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		logrus.Warnf("Error parsing flag: %v", err)
	}
	err = flag.Set("logtostderr", "true")
	if err != nil {
		logrus.Fatalf("Error setting glog flag: %v", err)
	}

	app := cli.NewApp()
	app.Name = "kdmp"
	app.Usage = "Kubernetes Data Management Platform (KDMP)"
	// TODO: Add compile time version to kdmp
	//app.Version = "v1"
	app.Action = run

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "Enable verbose logging",
		},
		cli.StringFlag{
			Name:  "rest-port",
			Usage: "REST port for kdmp",
		},
		cli.StringFlag{
			Name:  "grpc-port",
			Usage: "gRPC port for kdmp",
		},
		cli.StringFlag{
			Name:  "grpc-gateway-port",
			Usage: "gRPC gateway port for kdmp",
		},
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatalf("Error starting stork: %v", err)
	}
}

func run(c *cli.Context) {
	logrus.Infof("Starting Kubernetes Data Management Platform")

	verbose := c.Bool("verbose")
	if verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	c.String("rest-port")
}
