package influxdb_test

import (
	"context"
	"testing"

	influxdb "github.com/devarismeroxa/conduit-connector-influxdb"
	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := influxdb.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
