package service

import (
	"fmt"
	"github.com/foursquare/fsgo/net/discovery"
)

// KeyFunc defines the function signature for functions which can map
// ServiceInstances onto string keys
type KeyFunc func(*discovery.ServiceInstance) string

// Spec is a KeyFunc which maps a ServiceInstance onto ServiceInstance.Spec()
func Spec(serviceInstance *discovery.ServiceInstance) string {
	return serviceInstance.Spec()
}

var _ KeyFunc = Spec

// HttpAddress is a KeyFunc which maps a ServiceInstance onto a well-formed http or https URL
func HttpAddress(serviceInstance *discovery.ServiceInstance) string {
	if serviceInstance.SslPort != nil {
		return fmt.Sprintf("https://%s:%d", serviceInstance.Address, *serviceInstance.SslPort)
	} else if serviceInstance.Port != nil {
		return fmt.Sprintf("http://%s:%d", serviceInstance.Address, *serviceInstance.Port)
	}

	return fmt.Sprintf("http://%s", serviceInstance.Address)
}

var _ KeyFunc = HttpAddress

// InstanceId is a KeyFunc which maps a service instance to its unique identifier
func InstanceId(serviceInstance *discovery.ServiceInstance) string {
	return serviceInstance.Id
}

var _ KeyFunc = InstanceId

// Keys defines the method set for types which can receive the output of a KeyFunc
type Keys interface {
	Add(string)
}
