package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/pkg/options"
	"github.com/portworx/kdmp/cmd/3ncryptor/sdk"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	baseMountPath = "/var/lib/osd/mounts/"
)

var (
	namespace        string
	port             string
	auth_token       string
	dryRun           bool
	includeEncrypted bool
	volume_ids       string
	enc_secret       string
	snapshot_suffix  string = "-encryptorsnap"
	encrypted_suffix string = "-encrypted"
)

func getSnapName(volume_name string) string {
	return volume_name + snapshot_suffix
}

func getEncryptedVolName(volume_name string) string {
	return volume_name + encrypted_suffix
}

func listVolumes(namespace string) ([]*api.Volume, error) {
	vd := sdk.GetVolumeDriver()
	return vd.Enumerate(&api.VolumeLocator{VolumeLabels: map[string]string{"namespace": namespace}}, nil)
}

func inspectVolumes(volumeIds []string) ([]*api.Volume, error) {
	vd := sdk.GetVolumeDriver()
	return vd.Inspect(volumeIds)
}

func getSnapshot(vol *api.Volume) (*api.Volume, error) {
	vd := sdk.GetVolumeDriver()
	snapName := getSnapName(vol.Locator.Name)
	snaps, err := vd.Inspect([]string{snapName})
	if err != nil {
		return nil, err
	}

	if len(snaps) == 0 {
		return &api.Volume{}, fmt.Errorf("no snap found for: %v", snapName)

	}
	return snaps[0], err
}

func attachVol(vol *api.Volume, options map[string]string) error {
	vd := sdk.GetVolumeDriver()
	_, err := vd.Attach(vol.Id, options)
	return err
}

func detachVol(vol *api.Volume, secret string) error {
	vd := sdk.GetVolumeDriver()
	return vd.Detach(vol.Id, map[string]string{options.OptionsUnmountBeforeDetach: "true"})
}

func mountVol(vol *api.Volume, path string, secret string) error {
	vd := sdk.GetVolumeDriver()
	return vd.Mount(vol.Id, path, nil)
}

func createVol(locator *api.VolumeLocator, spec *api.VolumeSpec) (*api.Volume, error) {
	vd := sdk.GetVolumeDriver()

	locator.VolumeLabels["encryptor"] = "true"
	spec.Encrypted = true

	vol_id, err := vd.Create(locator, nil, spec)
	if err != nil {
		return &api.Volume{}, err
	}

	vol, err := vd.Inspect([]string{vol_id})
	if err != nil {
		return &api.Volume{}, err
	}

	return vol[0], nil
}

func snapVolume(vol *api.Volume, readonly bool, noRetry bool) (*api.Volume, error) {
	vd := sdk.GetVolumeDriver()

	locator := &api.VolumeLocator{
		Name:         getSnapName(vol.Locator.Name),
		VolumeLabels: vol.Locator.VolumeLabels,
	}
	locator.VolumeLabels["encryptor"] = "true"

	snap_id, err := vd.Snapshot(vol.Id, readonly, locator, noRetry)
	if err != nil {
		return &api.Volume{}, err
	}

	snap, err := vd.Inspect([]string{snap_id})
	if err != nil {
		return &api.Volume{}, err
	}

	return snap[0], nil
}

func cloneVol(vol *api.Volume, volName string) error {
	vd := sdk.GetVolumeDriver()

	locator := &api.VolumeLocator{
		Name:         volName,
		VolumeLabels: vol.Locator.VolumeLabels,
	}

	id, err := vd.Snapshot(vol.Id, false, locator, false)
	if err != nil {
		return err
	}

	vols, err := vd.Inspect([]string{id})
	if err != nil {
		return fmt.Errorf("failed to inspect cloned volume: %v", err)
	}

	if len(vols) != 0 {
		vols[0].Locator.VolumeLabels = vol.Locator.VolumeLabels
		err := vd.Set(id, vols[0].Locator, vols[0].Spec)
		if err != nil {
			return fmt.Errorf("failed to set volumelabels on clone: %v", err)
		}
	}

	return nil
}

func deleteVol(vol *api.Volume) error {
	vd := sdk.GetVolumeDriver()
	return vd.Delete(vol.Id)
}

func rsyncVol(src string, dest string) error {
	cmd := exec.Command("rsync", "-actv", src, dest)
	return cmd.Run()
}

func rollBack(snapVol, encVol *api.Volume, origVolName string) error {
	if encVol != nil {
		logrus.Infof("Deleting created encrypted volume: %v", encVol.Locator.Name)
		if err := deleteVol(encVol); err != nil {
			logrus.Errorf("delete failed for %s with: %v", encVol.Locator.Name, err)
		}
	}

	if snapVol != nil {
		logrus.Infof("Restoring original volume %v from: %v", origVolName, snapVol.Locator.Name)
		if err := cloneVol(snapVol, origVolName); err != nil {
			return fmt.Errorf("restore original volume %s failed: %v", origVolName, err)
		}
	}
	return nil
}

func main() {
	if err := NewCommand().Execute(); err != nil {
		os.Exit(1)
	}
}

func NewCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use:   "encryptor",
		Short: "Encryptor tool for unencrypted portworx volumes",
	}

	cmds.PersistentFlags().StringVarP(&namespace, "namespace", "n", "", "Namespace for this command")
	cmds.PersistentFlags().StringVarP(&port, "port", "p", "9001", "Port to talk (default: 9001)")
	cmds.PersistentFlags().StringVarP(&auth_token, "auth_token", "t", "", "Auth token for this command")

	cmds.AddCommand(
		newEncryptCommand(),
		newSnapCommand(),
		newRollbackCommand(),
	)
	cmds.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		util.CheckErr(err)
		return nil
	}

	return cmds
}

func newSnapCommand() *cobra.Command {
	snapCommand := &cobra.Command{
		Use:   "snap",
		Short: "Start snapshots",
		Run: func(c *cobra.Command, args []string) {
			if len(volume_ids) == 0 && len(namespace) == 0 {
				logrus.Errorf("namespace is required")
				os.Exit(1)
				return
			}

			if len(volume_ids) == 0 && len(namespace) != 0 {
				logrus.Infof("Using namespace: %s", namespace)
			}

			if err := sdk.InitVolDriver("127.0.0.1", port, auth_token); err != nil {
				//util.CheckErr(err)
				logrus.Errorf("failed to initialize SDK driver: %v", err)
				return
			}

			var (
				volumes []*api.Volume
				err     error
			)

			if len(volume_ids) != 0 {
				volumes, err = inspectVolumes(strings.Split(volume_ids, ","))
				if err != nil {
					logrus.Errorf("listvols failed with: %v", err)
					return
				}
			} else {
				volumes, err = listVolumes(namespace)
				if err != nil {
					logrus.Errorf("listvols failed with: %v", err)
					return
				}
			}

			for _, vol := range volumes {

				if !includeEncrypted && vol.Spec.Encrypted {
					continue
				}

				logrus.Infof("Snapshotting volume: %v", vol.Locator.Name)
				if !dryRun {
					_, err = snapVolume(vol, true, true)
					if err != nil {
						logrus.Errorf("snapVol failed with: %v", err)
						return
					}
				}
			}
		},
	}
	snapCommand.PersistentFlags().StringVarP(&volume_ids, "volume_ids", "v", "", "if provided, will use volume id instead of namespace")
	snapCommand.PersistentFlags().BoolVarP(&dryRun, "dryrun", "d", false, "if true, will dry-run operations")
	snapCommand.PersistentFlags().BoolVarP(&includeEncrypted, "include_encrypted", "s", false, "if true, will include secure volumes for re-encryption")
	return snapCommand
}

func newRollbackCommand() *cobra.Command {
	rollbackCommand := &cobra.Command{
		Use:   "rollback",
		Short: "Start rollback",
		Run: func(c *cobra.Command, args []string) {
			var (
				volumes []*api.Volume
				err     error
			)

			if len(volume_ids) == 0 && len(namespace) == 0 {
				logrus.Errorf("namespace is required")
				os.Exit(1)
				return
			}

			if len(volume_ids) == 0 && len(namespace) != 0 {
				logrus.Infof("Using namespace: %s", namespace)
			}

			if err := sdk.InitVolDriver("127.0.0.1", port, auth_token); err != nil {
				util.CheckErr(err)
				return
			}

			if len(volume_ids) != 0 {
				volumes, err = inspectVolumes(strings.Split(volume_ids, ","))
				logrus.Infof("Inspect")
				if err != nil {
					logrus.Errorf("listvols failed with: %v", err)
					return
				}
			} else {
				volumes, err = listVolumes(namespace)
				if err != nil {
					logrus.Errorf("listvols failed with: %v", err)
					return
				}
			}

			for _, vol := range volumes {
				var (
					snapVol, encVol *api.Volume
					origVolName     string = vol.Locator.Name
				)

				if len(volume_ids) == 0 && strings.Contains(vol.Locator.Name, snapshot_suffix) && strings.Contains(vol.Locator.Name, encrypted_suffix) {
					continue
				}

				if strings.Contains(vol.Locator.Name, snapshot_suffix) {
					origVolName = strings.Split(vol.Locator.Name, snapshot_suffix)[0]
				} else if strings.Contains(vol.Locator.Name, encrypted_suffix) {
					origVolName = strings.Split(vol.Locator.Name, encrypted_suffix)[0]
				}

				logrus.Infof("Rolling back volume: %v", origVolName)

				snapVol = nil
				if !strings.Contains(vol.Locator.Name, snapshot_suffix) && !strings.Contains(vol.Locator.Name, encrypted_suffix) {
					snaps, err := inspectVolumes([]string{getSnapName(origVolName)})
					if err != nil {
						logrus.Errorf("Failed to find the encryptor snap for %v", origVolName)
						return
					}

					if len(snaps) != 0 {
						snapVol = snaps[0]
					}
				}
				encVol = nil
				encvols, err := inspectVolumes([]string{getEncryptedVolName(origVolName)})
				if err != nil {
					logrus.Errorf("Failed to find the encrypted volume for: %v", origVolName)
					return
				}

				if len(encvols) != 0 {
					encVol = encvols[0]
				}

				if snapVol != nil {
					logrus.Infof("Deleting original volume")
					err := deleteVol(vol)
					if err != nil {
						logrus.Errorf("Failed to delete original volume")
						return
					}

					err = rollBack(snapVol, encVol, origVolName)
					if err != nil {
						logrus.Errorf("Failed to rollback volume %v", err)
					}
				}

			}
		},
	}
	rollbackCommand.PersistentFlags().StringVarP(&volume_ids, "volume_ids", "v", "", "if provided, will use volume id instead of namespace")
	rollbackCommand.PersistentFlags().BoolVarP(&dryRun, "dryrun", "d", false, "if true, will dry-run operations")
	rollbackCommand.PersistentFlags().BoolVarP(&includeEncrypted, "include_encrypted", "s", false, "if true, will include secure volumes for re-encryption")
	return rollbackCommand
}

func newEncryptCommand() *cobra.Command {
	encCommand := &cobra.Command{
		Use:   "encrypt",
		Short: "Start encryption",
		Run: func(c *cobra.Command, args []string) {
			if len(volume_ids) == 0 && len(namespace) == 0 {
				logrus.Errorf("namespace is required")
				os.Exit(1)
				return
			}

			if len(volume_ids) == 0 && len(namespace) != 0 {
				logrus.Infof("Using namespace: %s", namespace)
			}

			if err := sdk.InitVolDriver("127.0.0.1", port, auth_token); err != nil {
				util.CheckErr(err)
				return
			}

			var (
				volumes []*api.Volume
				err     error
			)

			if len(volume_ids) != 0 {
				volumes, err = inspectVolumes(strings.Split(volume_ids, ","))
				if err != nil {
					logrus.Errorf("listvols failed with: %v", err)
					return
				}
			} else {
				volumes, err = listVolumes(namespace)
				if err != nil {
					logrus.Errorf("listvols failed with: %v", err)
					return
				}
			}

			for _, vol := range volumes {
				if vol.AttachedOn != "" && vol.AttachedState != api.AttachState_ATTACH_STATE_INTERNAL {
					logrus.Errorf("volume %v is attached, please make sure your apps are scaled down.", vol.Locator.Name)
					return
				}
			}

			for _, vol := range volumes {
				var (
					encVol, snapVol *api.Volume
					origVolName     string = vol.Locator.Name
				)

				if !includeEncrypted && vol.Spec.Encrypted || strings.Contains(vol.Locator.Name, encrypted_suffix) || strings.Contains(vol.Locator.Name, snapshot_suffix) {
					continue
				}

				logrus.Infof("Locating snapshot for vol: %v", vol.Locator.Name)
				snapVol, err := getSnapshot(vol)
				if err != nil {
					logrus.Errorf("getSnapshot failed for %v with: %v", vol.Locator.Name, err)
					return
				}

				logrus.Infof("Creating encrypted volume for: %v", vol.Locator.Name)
				locator := &api.VolumeLocator{
					Name:         getEncryptedVolName(vol.Locator.Name),
					VolumeLabels: vol.Locator.VolumeLabels,
				}

				newSpec := vol.Spec
				newSpec.Passphrase = enc_secret

				if !dryRun {
					encVol, err = createVol(locator, newSpec)
					if err != nil {
						logrus.Errorf("createVol failed with: %v", err)
						return
					}
				}

				logrus.Infof("Attaching snapshot: %v", snapVol.Locator.Name)
				if !dryRun {
					if err := attachVol(snapVol, map[string]string{options.OptionsSecret: enc_secret}); err != nil {
						logrus.Errorf("attachVol failed to attach snapshot %v with: %v", snapVol.Locator.Name, err)
						return
					}
				}

				logrus.Infof("Attaching encrypted volume: %v", encVol.Locator.Name)
				if !dryRun {
					if err := attachVol(encVol, map[string]string{options.OptionsSecret: enc_secret}); err != nil {
						logrus.Errorf("attachVol failed to attach encrypted vol %v with: %v", encVol.Locator.Name, err)
						return
					}
				}

				dir := filepath.Join(baseMountPath, snapVol.Locator.Name)
				logrus.Infof("Creating directory: %v", dir)
				if !dryRun {
					if err := os.MkdirAll(dir, os.ModePerm); err != nil {
						logrus.Errorf("Mkdir failed to create dir: %v with: %v", dir, err)
						return
					}
				}

				logrus.Infof("Mounting snapshot: %v at %v", snapVol.Locator.Name, dir)
				if !dryRun {
					if err := mountVol(snapVol, dir, enc_secret); err != nil {
						logrus.Errorf("mountVol failed to mount snapshot %v with: %v", snapVol.Locator.Name, err)
						return
					}
				}

				encDir := filepath.Join(baseMountPath, encVol.Locator.Name)
				logrus.Infof("Creating directory: %v", encDir)
				if !dryRun {
					if err := os.MkdirAll(encDir, os.ModePerm); err != nil {
						logrus.Errorf("Mkdir failed to create dir: %v with: %v", encDir, err)
						return
					}
				}

				logrus.Infof("Mounting volume: %v at %v", vol.Locator.Name, encDir)
				if !dryRun {
					if err := mountVol(encVol, encDir, enc_secret); err != nil {
						logrus.Errorf("mountVol failed to mount encrypted vol %v with: %v", encVol.Locator.Name, err)
						return
					}
				}

				logrus.Infof("Rsync snapshot %v into encrypted volume: %v", snapVol.Locator.Name, encVol.Locator.Name)
				if !dryRun {
					if err := rsyncVol(dir, encDir); err != nil {
						logrus.Errorf("rsyncVol failed to rsync data between '%s' and '%s'", dir, encDir)
						return
					}
				}

				logrus.Infof("Detaching and unmounting snapshot: %v", snapVol.Locator.Name)
				if !dryRun {
					if err := detachVol(snapVol, enc_secret); err != nil {
						logrus.Errorf("detachVol failed to detach and unmount snapshot %v with: %v", snapVol.Locator.Name, err)
						return
					}
				}

				logrus.Infof("Detaching and unmounting encrypted volume: %v", encVol.Locator.Name)
				if !dryRun {
					if err := detachVol(encVol, enc_secret); err != nil {
						logrus.Errorf("detachVol failed to detach and unmount encrypted volume %v with: %v", encVol.Locator.Name, err)
						return
					}
				}

				logrus.Infof("Deleting original volume: %v", vol.Locator.Name)
				if !dryRun {
					if err := deleteVol(vol); err != nil {
						logrus.Errorf("deleteVol failed to delete volume: %v with %v", vol.Locator.Name, err)
						return
					}
				}

				logrus.Infof("Cloning encrypted volume: %v", encVol.Locator.Name)
				if !dryRun {
					if err := cloneVol(encVol, origVolName); err != nil {
						logrus.Errorf("cloneVol  failed to clone volume: %v with %v", encVol.Locator.Name, err)
						return
					}
				}

				logrus.Infof("Deleting encrypted volume: %v", encVol.Locator.Name)
				if !dryRun {
					if err := deleteVol(encVol); err != nil {
						logrus.Errorf("deleteVol failed to delete volume: %v with %v", encVol.Locator.Name, err)
						return
					}
				}
			}
		},
	}
	encCommand.PersistentFlags().StringVarP(&volume_ids, "volume_ids", "v", "", "if provided, will use volume id instead of namespace")
	encCommand.PersistentFlags().StringVarP(&enc_secret, "enc_secret", "s", "", "(required) encryption secret")
	encCommand.PersistentFlags().BoolVarP(&dryRun, "dryrun", "d", false, "if true, will dry-run operations")
	encCommand.PersistentFlags().BoolVarP(&includeEncrypted, "include_encrypted", "i", false, "if true, will include secure volumes for re-encryption")
	return encCommand
}
