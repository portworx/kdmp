package main

import (
	"fmt"
	"os"

	"context"
	"github.com/kopia/kopia/repo"
	_ "github.com/kopia/kopia/snapshot"
	_ "github.com/kopia/kopia/fs"
	_ "github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/repo/blob"
	"github.com/sirupsen/logrus"
	"github.com/kopia/kopia/repo/blob/s3"
)

const (
	s3Endpoint               = "minio.portworx.dev"
	awsAccessKeyIDEnvKey     = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyEnvKey = "AWS_SECRET_ACCESS_KEY" //nolint:gosec
	dataFileName             = "data"
)

func main() {
	repoDir := "test"
	bucket := "pk-kopia"
	password := "123456"

	ctx := context.Background()
	// First get the storage blob
	blob, err := getStorageHandler(ctx, repoDir, bucket)
	if err != nil {
		logrus.Errorf("%v", err)
		os.Exit(1)
	}

	// Now initialize the repo
	if err := repo.Initialize(ctx, blob, &repo.NewRepositoryOptions{}, password); err != nil {
		/*if !errors.Is(iErr, repo.ErrAlreadyInitialized) {
			return errors.Wrap(iErr, "repo is already initialized")
		}*/
		logrus.Errorf("error initialzing reo: %v", err)
	}

	logrus.Infof("connecting to existing repository")
}

func getStorageHandler(ctx context.Context, repoDir, bucket string) (st blob.Storage, err error) {
	s3Opts := &s3.Options{
		BucketName:      bucket,
		Prefix:          repoDir,
		Endpoint:        s3Endpoint,
		AccessKeyID:     os.Getenv(awsAccessKeyIDEnvKey),
		SecretAccessKey: os.Getenv(awsSecretAccessKeyEnvKey),
	}
	st, err = s3.New(ctx, s3Opts)

	return st, fmt.Errorf("error fetching : %v", err)
}