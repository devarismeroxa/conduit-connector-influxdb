package main

import (
	influxdb "github.com/devarismeroxa/conduit-connector-influxdb"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(influxdb.Connector)
}
