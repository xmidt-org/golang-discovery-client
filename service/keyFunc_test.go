package service

import (
	"github.com/foursquare/fsgo/net/discovery"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	port     = 1234
	sslPort  = 2345
	testData = []struct {
		serviceInstance     discovery.ServiceInstance
		expectedHttpAddress string
	}{
		{discovery.ServiceInstance{Id: "1", Address: "localhost", Port: &port}, "http://localhost:1234"},
		{discovery.ServiceInstance{Id: "2", Address: "foobar.com", Port: &port}, "http://foobar.com:1234"},
		{discovery.ServiceInstance{Id: "3", Address: "124.56.7.8", Port: &port}, "http://124.56.7.8:1234"},
		{discovery.ServiceInstance{Id: "4", Address: "localhost", SslPort: &sslPort}, "https://localhost:2345"},
		{discovery.ServiceInstance{Id: "5", Address: "foobar.com", SslPort: &sslPort}, "https://foobar.com:2345"},
		{discovery.ServiceInstance{Id: "6", Address: "124.56.7.8", SslPort: &sslPort}, "https://124.56.7.8:2345"},

		// favor the SSL port over the regular port when both are supplied
		{discovery.ServiceInstance{Id: "7", Address: "localhost", Port: &port, SslPort: &sslPort}, "https://localhost:2345"},
		{discovery.ServiceInstance{Id: "8", Address: "foobar.com", Port: &port, SslPort: &sslPort}, "https://foobar.com:2345"},
		{discovery.ServiceInstance{Id: "9", Address: "124.56.7.8", Port: &port, SslPort: &sslPort}, "https://124.56.7.8:2345"},

		// Take the Address when no port is given
		{discovery.ServiceInstance{Id: "10", Address: "localhost"}, "http://localhost"},
	}
)

func TestSpec(t *testing.T) {
	assert := assert.New(t)

	for _, record := range testData {
		actual := Spec(&record.serviceInstance)
		expected := record.serviceInstance.Spec()
		assert.Equal(expected, actual)
	}
}

func TestHttpAddress(t *testing.T) {
	assert := assert.New(t)

	for _, record := range testData {
		actual := HttpAddress(&record.serviceInstance)
		assert.Equal(record.expectedHttpAddress, actual)
	}
}

func TestInstanceId(t *testing.T) {
	assert := assert.New(t)

	for _, record := range testData {
		actual := InstanceId(&record.serviceInstance)
		assert.Equal(record.serviceInstance.Id, actual)
	}
}
