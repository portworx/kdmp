package dataexport

import (
	"context"
	"reflect"
	"time"

	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var (
	resyncPeriod = 30 * time.Second
)

// Controller is a k8s controller that handles DataExport resources.
type Controller struct {
	client runtimeclient.Client
}

// NewController returns a new instance of the controller.
func NewController(mgr manager.Manager) (*Controller, error) {
	return &Controller{
		client: mgr.GetClient(),
	}, nil
}

// Init Initialize the application backup controller
func (c *Controller) Init(mgr manager.Manager) error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	// Create a new controller
	ctrl, err := controller.New("data-export-controller", mgr, controller.Options{Reconciler: c})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource
	if err = ctrl.Watch(&source.Kind{Type: &kdmpapi.DataExport{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	// Watch Jobs and enqueue owning DataExport key
	return ctrl.Watch(&source.Kind{Type: &batchv1.Job{}}, &handler.EnqueueRequestForOwner{OwnerType: &kdmpapi.DataExport{}, IsController: true})
}

// Reconcile reads that state of the cluster for an object and makes changes based on the state read
// and what is in the Spec.
//
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
//
func (c *Controller) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	logrus.Debugf("Reconciling GroupVolumeSnapshot %s/%s", request.Namespace, request.Name)

	// Fetch the GroupSnapshot instance
	groupSnapshot := &kdmpapi.DataExport{}
	err := c.client.Get(context.TODO(), request.NamespacedName, groupSnapshot)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: 10 * time.Second}, err
	}

	if err = c.sync(context.TODO(), groupSnapshot); err != nil {
		return reconcile.Result{RequeueAfter: 3 * time.Second}, err
	}

	return reconcile.Result{RequeueAfter: resyncPeriod}, nil
}

func (c *Controller) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    kdmpapi.DataExportResourceName,
		Plural:  kdmpapi.DataExportResourcePlural,
		Group:   kdmpapi.SchemeGroupVersion.Group,
		Version: kdmpapi.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(kdmpapi.DataExport{}).Name(),
	}
	err := apiextensions.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRD(resource, 10*time.Second, 2*time.Minute)
}
