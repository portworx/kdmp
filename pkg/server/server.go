package server

import (
	"net/http"

	"github.com/libopenstorage/openstorage/api/server/sdk"
	"github.com/libopenstorage/openstorage/volume"
)

const (
	// KdmpSdkSocket is the openstorage SDK socket path
	KdmpSdkSocket = "/var/lib/osd/driver/kdmp.sock"
	// KdmpDriverName is the name of the openstorage driver
	KdmpDriverName = "kdmp"
)

// Config defines the configuration which is used for setting up
// openstorage REST and GRPC endpoints for backup
type Config struct {
	// Name of the backup driver
	Name string
	// BasePath for the unix server
	BasePath string
	// HttpPort for the REST server
	HttpPort uint16
	// GrpcPort for the gRPC server
	GrpcPort string
	// GrpcGatewayPort for the gRPC REST gateway
	GrpcGatewayPort string
	// BackupDriver is the openstorage Driver implementation
	BackupDriver volume.VolumeDriver
}

// Server is an implementation of the gRPC and REST interface
type Server struct {
	config     *Config
	unixServer *http.Server
	restServer *http.Server
	grpcServer *sdk.Server
}

func New(config *Config) (*Server, error) {
	var err error
	s := &Server{
		config: config,
	}
	// Setup the sdk grpc server
	sdkServerConf := &sdk.ServerConfig{
		Net:      "tcp",
		Address:  ":" + config.GrpcPort,
		Socket:   KdmpSdkSocket,
		RestPort: config.GrpcGatewayPort,
	}

	s.grpcServer, err = sdk.New(sdkServerConf)
	if err != nil {
		return nil, err
	}
	s.grpcServer.UseVolumeDrivers(
		map[string]volume.VolumeDriver{KdmpDriverName: config.BackupDriver},
	)
	return s, nil
}

func (s *Server) Start() error {
	var err error
	s.unixServer, s.restServer, err = server.StartBackupMgmtAPI(
		KdmpDriverName,
		KdmpSdkSocket,
		s.config.BasePath,
		s.config.HttpPort,
		false, // TODO: add support for auth
	)
	if err != nil {
		return err
	}
	err = s.grpcServer.Start()
	return err
}

func (s *Server) Stop() error {
	s.grpcServer.Stop()
	s.unixServer.Shutdown()
	s.restServer.Shutdown()
}
