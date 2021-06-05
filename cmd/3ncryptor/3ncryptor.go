package main

import (
	"flag"
	"github.com/libopenstorage/openstorage/api"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
	"os"
)

var (
	namespace          string
)

// listvols list all volumes in provided namespace
func listvols(namespace string) ([]*api.Volume, error) {
	return nil, nil
}

func attachVol(vol *api.Volume) error {
	return nil
}

func createVol(vol *api.Volume) (*api.Volume, error) {
	return nil, nil
}

func snapVol(vol *api.Volume) (*api.Volume, error) {
	return nil, nil
}

func cloneVol(vol *api.Volume) (*api.Volume, error) {
	return nil, nil
}

func main() {
	logrus.Infof("Hello from 3ncryptor")
	if err := NewCommand().Execute(); err != nil {
		os.Exit(1)
	}
}


// NewCommand returns a restic command wrapper
func NewCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use:   "restic_executor",
		Short: "a command executor for long running restic commands",
	}

	cmds.PersistentFlags().StringVarP(&namespace, "namespace", "n", "", "Namespace for this command")

	cmds.AddCommand(
		newEncryptCommand(),
	)
	cmds.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		util.CheckErr(err)
		return nil
	}

	return cmds
}

func newEncryptCommand() *cobra.Command {
	encCommand := &cobra.Command{
		Use:   "encrypt",
		Short: "Start encryption",
		Run: func(c *cobra.Command, args []string) {
			logrus.Infof("Hello from 3ncryptor: encrypt")

		},
	}
	return encCommand
}
