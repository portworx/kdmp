package utils

// Move this file to sched-ops once the location of DataExport CRD is finalized
import (
	"fmt"
	"os"
	"sync"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmpclientset "github.com/portworx/kdmp/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	instance Ops
	once     sync.Once
)

// Ops is an interface to KDMP operations.
type Ops interface {
	DataExportOps

	// SetConfig sets the config and resets the client
	SetConfig(config *rest.Config)
}

// Instance returns a singleton instance of the client.
func Instance() Ops {
	once.Do(func() {
		if instance == nil {
			instance = &Client{}
		}
	})
	return instance
}

// SetInstance replaces the instance with the provided one. Should be used only for testing purposes.
func SetInstance(i Ops) {
	instance = i
}

// New builds a new operator client.
func New(c kdmpclientset.Interface) *Client {
	return &Client{
		kst: c,
	}
}

// NewForConfig builds a new operator client for the given config.
func NewForConfig(c *rest.Config) (*Client, error) {
	kstClient, err := kdmpclientset.NewForConfig(c)
	if err != nil {
		return nil, err
	}

	return &Client{
		kst: kstClient,
	}, nil
}

// NewInstanceFromConfigFile returns new instance of client by using given
// config file
func NewInstanceFromConfigFile(config string) (Ops, error) {
	newInstance := &Client{}
	err := newInstance.loadClientFromKubeconfig(config)
	if err != nil {
		return nil, err
	}
	return newInstance, nil
}

// Client is a wrapper for the operator client.
type Client struct {
	config *rest.Config
	kst    kdmpclientset.Interface
}

// SetConfig sets the config and resets the client
func (c *Client) SetConfig(cfg *rest.Config) {
	c.config = cfg
	c.kst = nil
}

// initClient the k8s client if uninitialized
func (c *Client) initClient() error {
	if c.kst != nil {
		return nil
	}

	return c.setClient()
}

// setClient instantiates a client.
func (c *Client) setClient() error {
	var err error

	if c.config != nil {
		err = c.loadClient()
	} else {
		kubeconfig := os.Getenv("KUBECONFIG")
		if len(kubeconfig) > 0 {
			err = c.loadClientFromKubeconfig(kubeconfig)
		} else {
			err = c.loadClientFromServiceAccount()
		}

	}

	return err
}

// loadClientFromServiceAccount loads a k8s client from a ServiceAccount specified in the pod running px
func (c *Client) loadClientFromServiceAccount() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	c.config = config
	return c.loadClient()
}

func (c *Client) loadClientFromKubeconfig(kubeconfig string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return err
	}

	c.config = config
	return c.loadClient()
}

func (c *Client) loadClient() error {
	if c.config == nil {
		return fmt.Errorf("rest config is not provided")
	}

	var err error

	c.kst, err = kdmpclientset.NewForConfig(c.config)
	if err != nil {
		return err
	}

	return nil
}

// DataExportOps is an interface to perform kubernetes related operations on the kdmp resources.
type DataExportOps interface {
	// CreateDataExport create a provided DataExport object
	CreateDataExport(obj *kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error)
	// GetDataExport gets the DataExport object of the given name in the given namespace
	GetDataExport(name, namespace string) (*kdmpv1alpha1.DataExport, error)
	// UpdateDataExportStatus updates the status field of DataExport
	UpdateDataExportStatus(*kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error)
}

// CreateDataExport create a provided DataExport object
func (c *Client) CreateDataExport(obj *kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kst.KdmpV1alpha1().DataExports(obj.Namespace).Create(obj)
}

// GetDataExport gets the DataExport object of the given name in the given namespace
func (c *Client) GetDataExport(name, namespace string) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kst.KdmpV1alpha1().DataExports(namespace).Get(name, metav1.GetOptions{})
}

// UpdateDataExportStatus updates the status field of DataExport
func (c *Client) UpdateDataExportStatus(dataExport *kdmpv1alpha1.DataExport) (*kdmpv1alpha1.DataExport, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kst.KdmpV1alpha1().DataExports(dataExport.Namespace).UpdateStatus(dataExport)
}