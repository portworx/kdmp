/*
Copyright © 2019 Portworx

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

package util

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PxErrorMessage returns an error composed of the gRPC error status message
// and the message provided.
func PxErrorMessage(err error, msg string) error {
	gerr := FromError(err)
	return fmt.Errorf("%s: %s", msg, gerr.Message())
}

// PxErrorMessagef is like PxErrorMessage, but also adds formatted string support
func PxErrorMessagef(err error, format string, args ...string) error {
	return PxErrorMessage(err, fmt.Sprintf(format, args))
}

// PrintPxErrorMessagef prints the Portworx error message to Stderr
func PrintPxErrorMessagef(err error, format string, args ...string) {
	Eprintf("%v\n", PxErrorMessagef(err, format, args...))
}

// PxError extracts and returns the message found in the gRPC error status
func PxError(err error) error {
	if err == nil {
		return err
	}
	gerr := FromError(err)

	return fmt.Errorf("%s", gerr.Message())
}

// RectifyErrorMessage changes cryptic messages
func RectifyErrorMessage(msg string) string {
	return msg
}

// IsErrorNotFound returns if the given error is due to not found
func IsErrorNotFound(err error) bool {
	return FromError(err).Code() == codes.NotFound
}

func IsErrorPermissionDenied(err error) bool {
	return FromError(err).Code() == codes.PermissionDenied
}

func FromError(err error) *status.Status {
	// From github.com/grpc-ecosystem/grpc-gateway/runtime/errors.go
	s, ok := status.FromError(err)
	if !ok {
		s = status.New(codes.Unknown, err.Error())
	}

	// Cleanup error which may be confusing for user
	switch s.Message() {
	case "Request unauthenticated with bearer":
		s = status.New(s.Code(), "Authentication information required")
	}

	return s
}
