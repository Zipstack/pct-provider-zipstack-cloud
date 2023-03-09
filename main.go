package main

import (
	"github.com/zipstack/pct-plugin-framework/schema"
	"github.com/zipstack/pct-plugin-framework/server"

	"github.com/zipstack/pct-provider-zmesh/plugin"
)

func main() {
	server.Serve(plugin.NewProvider, []func() schema.ResourceService{
		plugin.NewDatasourceResource,
		plugin.NewHypertableResource,
	})
}
