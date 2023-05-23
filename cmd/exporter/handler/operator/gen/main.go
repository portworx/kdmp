package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
)

const (
	src = "../../../../deploy"

	fileTpl = `// This is a generated file. DO NOT EDIT
package operator

import (
	"bytes"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

var (
	operatorServiceAccountYaml = __{{ .ServiceAccount }}__

	operatorDeploymentYaml = __{{ .Deployment }}__

	operatorClusterRoleYaml = __{{ .ClusterRole }}__

	operatorClusterRoleBindingYaml = __{{ .ClusterRoleBinding }}__
)

type Manifests struct {
	ServiceAccount     corev1.ServiceAccount
	Deployment         appsv1.Deployment
	ClusterRole        rbacv1.ClusterRole
	ClusterRoleBinding rbacv1.ClusterRoleBinding
}

func ParseOperatorManifests() (Manifests, error) {
	manifests := Manifests{}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBufferString(operatorServiceAccountYaml), 4096).Decode(&manifests.ServiceAccount); err != nil {
		return Manifests{}, err
	}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBufferString(operatorDeploymentYaml), 4096).Decode(&manifests.Deployment); err != nil {
		return Manifests{}, err
	}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBufferString(operatorClusterRoleYaml), 4096).Decode(&manifests.ClusterRole); err != nil {
		return Manifests{}, err
	}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBufferString(operatorClusterRoleBindingYaml), 4096).Decode(&manifests.ClusterRoleBinding); err != nil {
		return Manifests{}, err
	}
	return manifests, nil
}
`
)

var (
	verifyMap = map[string]runtime.Object{
		"serviceaccount.yaml":     &corev1.ServiceAccount{},
		"deployment.yaml":         &appsv1.Deployment{},
		"clusterrole.yaml":        &rbacv1.ClusterRole{},
		"clusterrolebinding.yaml": &rbacv1.ClusterRoleBinding{},
	}

	rawMap = map[string][]byte{}
)

func main() {
	err := runYamlReader()
	if err != nil {
		log.Fatal(err)
	}
}

func runYamlReader() error {
	fi, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("source is not a directory")
	}
	files, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if fi.IsDir() {
			continue
		}
		fpath, err := filepath.Abs(filepath.Join(src, fi.Name()))
		if err != nil {
			return err
		}
		fmt.Println("read file:", fpath)
		dstObj := verifyMap[fi.Name()]
		if dstObj == nil {
			continue
		}

		file, err := os.Open(fpath)
		if err != nil {
			return err
		}
		err = yaml.NewYAMLOrJSONDecoder(file, 4096).Decode(dstObj)
		if err != nil {
			return err
		}

		raw, err := os.ReadFile(fpath)
		if err != nil {
			return err
		}
		rawMap[fi.Name()] = raw
	}

	t, err := template.New("").Parse(fileTpl)
	if err != nil {
		return err
	}

	buff := bytes.NewBufferString("")
	err = t.Execute(buff, struct {
		ServiceAccount     string
		Deployment         string
		ClusterRole        string
		ClusterRoleBinding string
	}{
		ServiceAccount:     string(rawMap["serviceaccount.yaml"]),
		Deployment:         string(rawMap["deployment.yaml"]),
		ClusterRole:        string(rawMap["clusterrole.yaml"]),
		ClusterRoleBinding: string(rawMap["clusterrolebinding.yaml"]),
	})
	if err != nil {
		return err
	}

	outfile, err := os.OpenFile("installmanifests_generated.go", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	_, err = fmt.Fprint(outfile, strings.ReplaceAll(buff.String(), "__", "`"))
	return err
}
