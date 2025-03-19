package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
	influxdb "github.com/devarismeroxa/conduit-connector-influxdb"
)

func main() {
	sdk.Serve(influxdb.Connector)
}
