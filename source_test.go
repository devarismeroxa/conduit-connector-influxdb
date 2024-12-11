package influxdb_test

import (
	"context"
	"testing"

	influxdb "github.com/devarismeroxa/conduit-connector-influxdb"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := influxdb.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
