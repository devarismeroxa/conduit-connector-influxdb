package influxdb

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/influxdata/influxdb-client-go/v2/api"
)

type Destination struct {
	sdk.UnimplementedDestination

	config    DestinationConfig
	client    influxdb2.Client
	writeAPI  api.WriteAPI
	batchSize int
	batch     []*influxdb2.Point
}

type DestinationConfig struct {
	// Config includes parameters that are the same in the source and destination.
	Config
	// DestinationConfigParam must be either yes or no (defaults to yes).
	DestinationConfigParam string `validate:"inclusion=yes|no" default:"yes"`
}

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() config.Parameters {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination. Parameters can be generated from DestinationConfig with
	// paramgen.
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	// Configure is the first function to be called in a connector. It provides
	// the connector with the configuration that can be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The SDK will validate the configuration and populate default values
	// before calling Configure. If you need to do more complex validations you
	// can do them manually here.

	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, NewDestination().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	d.client = influxdb2.NewClient(d.config.URL, d.config.Token)
	d.writeAPI = d.client.WriteAPI(d.config.Org, d.config.Bucket)
	d.batchSize = d.config.BatchSize
	d.batch = make([]*influxdb2.Point, 0, d.batchSize)
	return nil
}

func (d *Destination) Open(_ context.Context) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.
	sdk.Logger(ctx).Info().Msg("Opening Destination...")
	return nil
}

func (d *Destination) Write(_ context.Context, _ []opencdc.Record) (int, error) {
	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).
	sdk.Logger(ctx).Info().Msgf("Writing %d records to InfluxDB", len(records))

	for i, record := range records {
		point, err := sdkRecordToInfluxDBPoint(record)
		if err != nil {
			return i, fmt.Errorf("failed to convert record to InfluxDB point: %w", err)
		}
		d.batch = append(d.batch, point)

		if len(d.batch) >= d.batchSize {
			if err := d.flushBatch(ctx); err != nil {
				return i, fmt.Errorf("failed to flush batch: %w", err)
			}
		}
	}
	return len(records), nil
}

func (d *Destination) Teardown(_ context.Context) error {
	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	sdk.Logger(ctx).Info().Msg("Tearing down Destination...")
	if len(d.batch) > 0 {
		if err := d.flushBatch(ctx); err != nil {
			return fmt.Errorf("failed to flush remaining batch: %w", err)
		}
	}
	d.client.Close()
	return nil
}

func (d *Destination) flushBatch(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msgf("Flushing %d points to InfluxDB", len(d.batch))
	for _, point := range d.batch {
		d.writeAPI.WritePoint(point)
	}
	d.writeAPI.Flush()
	d.batch = d.batch[:0]
	sdk.Logger(ctx).Info().Msg("Batch flushed successfully")
	return nil
}

func sdkRecordToInfluxDBPoint(record opencdc.Record) (*influxdb2.Point, error) {
	fields := make(map[string]interface{})
	for k, v := range record.Payload.After {
		fields[k] = v
	}

	tags := make(map[string]string)
	for k, v := range record.Metadata {
		tags[k] = v
	}

	return influxdb2.NewPoint(
		record.Key.(string),
		tags,
		fields,
		record.Timestamp,
	), nil
}
