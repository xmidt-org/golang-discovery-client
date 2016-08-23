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

// Keys defines the method set for types which can receive the output of a KeyFunc
type Keys interface {
	Add(string)
}

// KeySlice is analogous to sort.StringSlice.  It provides a sortable slice of
// service keys.  This type implements Keys, so it can be used as an intermediate
// step for processing a set of services.  Additionally, this type implements sort.Interface.
type KeySlice []string

func (ks *KeySlice) Add(key string) {
	*ks = append(*ks, key)
}

func (ks KeySlice) ToKeys(output Keys) {
	for _, key := range ks {
		output.Add(key)
	}
}

func (ks KeySlice) Len() int {
	return len(ks)
}

func (ks KeySlice) Less(i, j int) bool {
	return ks[i] < ks[j]
}

func (ks KeySlice) Swap(i, j int) {
	ks[i], ks[j] = ks[j], ks[i]
}

// KeyMap is a convenient map type which can store both the result of a KeyFunc
// and the associated ServiceInstance.  Using a KeyMap will automatically dedupe
// services, as all but the last one added will be discarded.
type KeyMap map[string]*discovery.ServiceInstance

// ToKeys invokes keys.Add() for each service key in this map.
// Unlike Instances.ToKeys(), this method does not take a KeyFunc
// as the keys are already present in this map.
func (km KeyMap) ToKeys(keys Keys) {
	for key, _ := range km {
		keys.Add(key)
	}
}
