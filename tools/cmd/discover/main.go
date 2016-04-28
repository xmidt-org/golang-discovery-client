package main

import (
	"flag"
	"fmt"
	"github.com/Comcast/golang-discovery-client/service"
	"github.com/Comcast/golang-discovery-client/service/cmd"
	"os"
)

func main() {
	cmd.ConfigureCommandLine()
	flag.Parse()
	discoveryBuilder := &service.DiscoveryBuilder{}
	cmd.ApplyCommandLine(discoveryBuilder)

	discovery, err := discoveryBuilder.NewDiscovery(&service.DefaultLogger{os.Stdout}, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to start discovery client: %v\n", err)
		os.Exit(1)
	}

	defer discovery.Close()
}
