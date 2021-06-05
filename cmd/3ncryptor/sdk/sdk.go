package sdk

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"os"

	httpclient "github.com/libopenstorage/openstorage/api/client"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	"github.com/libopenstorage/openstorage/volume"
	"github.com/sirupsen/logrus"
)

var (
	schedVolDriver volume.VolumeDriver
	token          string
	UseTLS         bool
	tokenEndPoint  string
	tlsConfig      *tls.Config
)

// InitVolDriver inits vol driver
func InitVolDriver(host string, port string, auth_token string) error {
	if len(auth_token) != 0 {
		token = auth_token
	}

	schedClient, err := getNewVolumeclient(host, port, "pxd")
	if err != nil {
		logrus.Errorf("Error while initializing PX CLI for scheduler")
		os.Exit(-1)
	}

	schedVolDriver = volumeclient.VolumeDriver(schedClient)
	return nil
}

func getNewVolumeclient(endpoint, port, driverName string) (*httpclient.Client, error) {
	return getNewAuthVolumeclient(endpoint, port, token, driverName)
}

// GetNewAuthVolumeclient create volume api client with auth token
func getNewAuthVolumeclient(endpoint, port, bearerToken, driverName string) (*httpclient.Client, error) {
	if len(endpoint) > 0 && len(port) > 0 {
		endpoint = buildHTTPSEndpoint(endpoint, port)
	} else if len(tokenEndPoint) > 0 {
		endpoint = tokenEndPoint
	}

	driverVersion := "v1"

	if driverName == "" {
		driverName = "pxd"
	}

	var (
		clnt *httpclient.Client
		err  error
	)
	if len(bearerToken) != 0 {
		clnt, err = volumeclient.NewAuthDriverClient(endpoint, driverName, driverVersion, bearerToken, "", driverName)
	} else {
		clnt, err = volumeclient.NewDriverClient(endpoint, driverName, driverVersion, driverName)
	}
	if err != nil {
		return nil, err
	}

	if UseTLS && !IsUnixDomainSocket(endpoint) {
		clnt.SetTLS(tlsConfig)
	}

	return clnt, nil
}

// BuildHTTPSEndpoint create http endpoint URL
func buildHTTPSEndpoint(host string, port string) string {
	if IsUnixDomainSocket(host) {
		return host
	}
	endpoint := &url.URL{}
	endpoint.Scheme = "http"
	endpoint.Host = fmt.Sprintf("%s:%s", host, port)

	if UseTLS {
		endpoint.Scheme = "https"
	}
	return endpoint.String()
}

// IsUnixDomainSocket is a helper method that checks if the host is a unix DS endpoint in the format unix://<path>
func IsUnixDomainSocket(host string) bool {
	// Check if it is a unix:// URL
	u, err := url.Parse(host)
	if err == nil {
		// Check if host just has an IP
		if u.Scheme == "unix" {
			return true
		}
		if !u.IsAbs() && net.ParseIP(host) == nil {
			return true
		}
	}
	return false
}

func GetVolumeDriver() volume.VolumeDriver {
	return schedVolDriver
}
