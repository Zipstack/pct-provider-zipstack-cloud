package main

import (
	"github.com/zipstack/pct-plugin-framework/schema"
	"github.com/zipstack/pct-plugin-framework/server"

	"github.com/zipstack/pct-provider-zipstack-cloud/plugin"
)

// Set while building the compiled binary.
var version string

func main() {
	server.Serve(version, plugin.NewProvider, []func() schema.ResourceService{
		plugin.NewDatasourceResource,
		plugin.NewHypertableLiveResource,
	})
}
