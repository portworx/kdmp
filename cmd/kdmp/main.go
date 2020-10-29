package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/portworx/kdmp/pkg/apis"
	"github.com/portworx/kdmp/pkg/controllers/dataexport"
	"github.com/portworx/kdmp/pkg/version"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	defaultLockObjectName      = "kdmp"
	defaultLockObjectNamespace = "kube-system"
	defaultLockLease           = 15 * time.Second
	defaultLockRenew           = 10 * time.Second
	defaultLockRetry           = 2 * time.Second
)

func main() {
	// TODO: review klog config

	app := cli.NewApp()
	app.Name = "kdmp"
	app.Usage = "Kubernetes data management platform (KDMP)"
	//app.Version = version.Version
	app.Action = run

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "Enable verbose logging",
		},
		cli.BoolTFlag{
			Name:  "leader-elect",
			Usage: "Enable leader election (default: true)",
		},
		cli.StringFlag{
			Name:  "lock-object-name",
			Usage: "Name for the lock object",
			Value: defaultLockObjectName,
		},
		cli.StringFlag{
			Name:  "lock-object-namespace",
			Usage: "Namespace for the lock object",
			Value: defaultLockObjectNamespace,
		},
		cli.StringSliceFlag{
			Name:  "enable-controllers",
			Usage: "Enable provided custom controllers",
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatalf("Error starting kdmp: %v", err)
	}
}

func run(c *cli.Context) {
	log.SetLevel(log.DebugLevel)

	v := version.Get()
	log.Infof("Starting kdmp: %s, build date %s", v.String(), v.BuildDate)

	mgrOpts := manager.Options{}
	if c.BoolT("leader-elect") {
		mgrOpts.LeaderElection = true
		mgrOpts.LeaderElectionID = c.String("lock-object-name")
		mgrOpts.LeaderElectionNamespace = c.String("lock-object-namespace")
		mgrOpts.LeaseDuration = durationPtr(defaultLockLease)
		mgrOpts.RenewDeadline = durationPtr(defaultLockRenew)
		mgrOpts.RetryPeriod = durationPtr(defaultLockRetry)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Error getting cluster config: %v", err)
	}

	// Create operator-sdk manager that will manage all controllers.
	mgr, err := manager.New(config, mgrOpts)
	if err != nil {
		log.Fatalf("Setup controller manager: %v", err)
	}

	// Setup scheme for controllers resources
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		log.Fatalf("Setup scheme for kdmp resources: %v", err)
	}

	if err := runApp(mgr); err != nil {
		log.Fatalf("Controller manager: %v", err)
	}
	os.Exit(0)
}

func runApp(mgr manager.Manager) error {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	stopCh := make(chan struct{}, 1)
	go func() {
		for {
			<-signalChan
			stopCh <- struct{}{}
		}
	}()

	de, err := dataexport.NewController(mgr)
	if err != nil {
		return fmt.Errorf("build DataExport controller: %s", err)
	}
	if err = de.Init(mgr); err != nil {
		return fmt.Errorf("init DataExport controller: %s", err)
	}

	log.Info("Starting controller manager")
	return mgr.Start(stopCh)
}

func durationPtr(in time.Duration) *time.Duration {
	return &in
}
