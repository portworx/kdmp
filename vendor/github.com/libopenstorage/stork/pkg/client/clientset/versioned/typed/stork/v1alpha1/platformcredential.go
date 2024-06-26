/*
Copyright 2018 Openstorage.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by client-gen. DO NOT EDIT.

package v1alpha1

import (
	"context"
	"time"

	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	scheme "github.com/libopenstorage/stork/pkg/client/clientset/versioned/scheme"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// PlatformCredentialsGetter has a method to return a PlatformCredentialInterface.
// A group's client should implement this interface.
type PlatformCredentialsGetter interface {
	PlatformCredentials(namespace string) PlatformCredentialInterface
}

// PlatformCredentialInterface has methods to work with PlatformCredential resources.
type PlatformCredentialInterface interface {
	Create(ctx context.Context, platformCredential *v1alpha1.PlatformCredential, opts v1.CreateOptions) (*v1alpha1.PlatformCredential, error)
	Update(ctx context.Context, platformCredential *v1alpha1.PlatformCredential, opts v1.UpdateOptions) (*v1alpha1.PlatformCredential, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.PlatformCredential, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.PlatformCredentialList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.PlatformCredential, err error)
	PlatformCredentialExpansion
}

// platformCredentials implements PlatformCredentialInterface
type platformCredentials struct {
	client rest.Interface
	ns     string
}

// newPlatformCredentials returns a PlatformCredentials
func newPlatformCredentials(c *StorkV1alpha1Client, namespace string) *platformCredentials {
	return &platformCredentials{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the platformCredential, and returns the corresponding platformCredential object, and an error if there is any.
func (c *platformCredentials) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.PlatformCredential, err error) {
	result = &v1alpha1.PlatformCredential{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("platformcredentials").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of PlatformCredentials that match those selectors.
func (c *platformCredentials) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.PlatformCredentialList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1alpha1.PlatformCredentialList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("platformcredentials").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested platformCredentials.
func (c *platformCredentials) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("platformcredentials").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a platformCredential and creates it.  Returns the server's representation of the platformCredential, and an error, if there is any.
func (c *platformCredentials) Create(ctx context.Context, platformCredential *v1alpha1.PlatformCredential, opts v1.CreateOptions) (result *v1alpha1.PlatformCredential, err error) {
	result = &v1alpha1.PlatformCredential{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("platformcredentials").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(platformCredential).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a platformCredential and updates it. Returns the server's representation of the platformCredential, and an error, if there is any.
func (c *platformCredentials) Update(ctx context.Context, platformCredential *v1alpha1.PlatformCredential, opts v1.UpdateOptions) (result *v1alpha1.PlatformCredential, err error) {
	result = &v1alpha1.PlatformCredential{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("platformcredentials").
		Name(platformCredential.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(platformCredential).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the platformCredential and deletes it. Returns an error if one occurs.
func (c *platformCredentials) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("platformcredentials").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *platformCredentials) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("platformcredentials").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched platformCredential.
func (c *platformCredentials) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.PlatformCredential, err error) {
	result = &v1alpha1.PlatformCredential{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("platformcredentials").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
