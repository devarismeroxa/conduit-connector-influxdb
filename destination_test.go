package influxdb_test

import (
	"context"
	"testing"

	sdk "github.com/ConduitIO/conduit-connector-sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDestination_Configure(t *testing.T) {
	d := NewDestination()
	cfg := map[string]string{
		"influxdb.url":    "http://localhost:8086",
		"influxdb.token":  "my-token",
		"influxdb.org":    "my-org",
		"influxdb.bucket": "my-bucket",
		"batch.size":      "50",
	}
	err := d.Configure(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8086", d.client.Options().ServerURL)
	assert.Equal(t, 50, d.batchSize)
}

func TestDestination_Write_Success(t *testing.T) {
	d := NewDestination()
	cfg := map[string]string{
		"influxdb.url":    "http://localhost:8086",
		"influxdb.token":  "my-token",
		"influxdb.org":    "my-org",
		"influxdb.bucket": "my-bucket",
		"batch.size":      "2",
	}
	err := d.Configure(context.Background(), cfg)
	require.NoError(t, err)

	d.client = influxdb2.NewClientWithOptions(cfg["influxdb.url"], cfg["influxdb.token"], influxdb2.DefaultOptions())
	d.writeAPI = d.client.WriteAPI(d.org, d.bucket)

	record1 := sdk.Record{
		Key: "measurement1",
		Payload: sdk.Change{After: map[string]interface{}{
			"field1": 42,
		}},
		Metadata: map[string]string{
			"tag1": "value1",
		},
	}
	record2 := sdk.Record{
		Key: "measurement2",
		Payload: sdk.Change{After: map[string]interface{}{
			"field2": 43,
		}},
		Metadata: map[string]string{
			"tag2": "value2",
		},
	}
}
