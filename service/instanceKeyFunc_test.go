package service

import (
	"github.com/foursquare/fsgo/net/discovery"
	"testing"
)

var (
	port     = 1234
	sslPort  = 2345
	testData = []struct {
		serviceInstance     discovery.ServiceInstance
		expectedHttpAddress string
	}{
		{discovery.ServiceInstance{Address: "localhost", Port: &port}, "http://localhost:1234"},
		{discovery.ServiceInstance{Address: "foobar.com", Port: &port}, "http://foobar.com:1234"},
		{discovery.ServiceInstance{Address: "124.56.7.8", Port: &port}, "http://124.56.7.8:1234"},
		{discovery.ServiceInstance{Address: "localhost", SslPort: &sslPort}, "https://localhost:2345"},
		{discovery.ServiceInstance{Address: "foobar.com", SslPort: &sslPort}, "https://foobar.com:2345"},
		{discovery.ServiceInstance{Address: "124.56.7.8", SslPort: &sslPort}, "https://124.56.7.8:2345"},

		// favor the SSL port over the regular port when both are supplied
		{discovery.ServiceInstance{Address: "localhost", Port: &port, SslPort: &sslPort}, "https://localhost:2345"},
		{discovery.ServiceInstance{Address: "foobar.com", Port: &port, SslPort: &sslPort}, "https://foobar.com:2345"},
		{discovery.ServiceInstance{Address: "124.56.7.8", Port: &port, SslPort: &sslPort}, "https://124.56.7.8:2345"},
	}
)

func TestSpec(t *testing.T) {
	for _, record := range testData {
		actual := Spec(&record.serviceInstance)
		expected := record.serviceInstance.Spec()

		if actual != expected {
			t.Errorf("Expected %s but got %s", expected, actual)
		}
	}
}

func TestHttpAddress(t *testing.T) {
	for _, record := range testData {
		actual := HttpAddress(&record.serviceInstance)

		if actual != record.expectedHttpAddress {
			t.Errorf("Expected %s but got %s", record.expectedHttpAddress, actual)
		}
	}
}
