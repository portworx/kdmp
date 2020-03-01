module github.com/portworx/kdmp

go 1.13

require (
	github.com/stretchr/testify v1.4.0
	golang.org/x/lint v0.0.0-20200130185559-910be7a94367 // indirect
	golang.org/x/tools v0.0.0-20200226224502-204d844ad48d // indirect
	honnef.co/go/tools v0.0.1-2020.1.3 // indirect
	k8s.io/api v0.16.6
	k8s.io/apimachinery v0.16.6
	k8s.io/client-go v0.16.6
	k8s.io/code-generator v0.16.6
)

replace (
	k8s.io/code-generator => k8s.io/code-generator v0.16.6
	sigs.k8s.io/controller-runtime => sigs.k8s.io/controller-runtime v0.4.0
)
