// Copyright © 2025 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package influxdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/devarismeroxa/conduit-connector-influxdb/destination"
	"github.com/devarismeroxa/conduit-connector-influxdb/source"
)

// Destination is an implementation of sdk.Destination that writes to InfluxDB.
type Destination struct {
	sdk.UnimplementedDestination

	config    destination.Config
	client    influxdb2.Client
	writeAPI  api.WriteAPI
	batchSize int
}

// NewDestination creates a new instance of the InfluxDB destination connector.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

// Parameters returns the required parameters for the destination connector.
func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

// Open initializes the connector and starts a connection to InfluxDB.
func (d *Destination) Open(ctx context.Context) error {
	// Initialize client
	options := influxdb2.DefaultOptions()
	options.SetBatchSize(uint(d.config.BatchSize))
	d.client = influxdb2.NewClientWithOptions(d.config.URL, d.config.Token, options)

	// Get write API
	d.writeAPI = d.client.WriteAPI(d.config.Org, d.config.Bucket)

	// Set up error handler for write API
	errorsCh := d.writeAPI.Errors()
	go func() {
		for err := range errorsCh {
			if err != nil {
				sdk.Logger(ctx).Error().Err(err).Msg("error writing to InfluxDB")
			}
		}
	}()

	return nil
}

// Write writes records to InfluxDB.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		// Skip delete operations for InfluxDB
		if record.Operation == opencdc.OperationDelete {
			sdk.Logger(ctx).Warn().Msg("delete operations are not supported by InfluxDB, skipping record")
			continue
		}

		point, err := d.recordToPoint(record)
		if err != nil {
			return i, fmt.Errorf("failed to convert record to point: %w", err)
		}

		d.writeAPI.WritePoint(point)
	}

	// Flush the batch
	d.writeAPI.Flush()

	return len(records), nil
}

// Teardown closes the connection to InfluxDB.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.client != nil {
		// Flush any pending writes
		d.writeAPI.Flush()
		d.client.Close()
		d.client = nil
		d.writeAPI = nil
	}
	return nil
}

// recordToPoint converts a Conduit record to an InfluxDB point.
func (d *Destination) recordToPoint(record opencdc.Record) (*write.Point, error) {
	// Extract data from record
	data, err := d.getStructuredData(record)
	if err != nil {
		return nil, fmt.Errorf("failed to get structured data from record: %w", err)
	}

	// Determine measurement name
	measurement, err := d.getMeasurement(record, data)
	if err != nil {
		return nil, fmt.Errorf("failed to determine measurement name: %w", err)
	}

	// Extract tags
	tags := make(map[string]string)
	if d.config.TagsField != "" {
		if tagsData, ok := data[d.config.TagsField].(map[string]string); ok {
			tags = tagsData
		} else if tagsData, ok := data[d.config.TagsField].(map[string]interface{}); ok {
			// Convert to string map
			for k, v := range tagsData {
				if stringVal, ok := v.(string); ok {
					tags[k] = stringVal
				} else {
					// Convert to string
					tags[k] = fmt.Sprintf("%v", v)
				}
			}
		} else if tagsData, ok := data[d.config.TagsField].(opencdc.StructuredData); ok {
			// Convert to string map
			for k, v := range tagsData {
				if stringVal, ok := v.(string); ok {
					tags[k] = stringVal
				} else {
					// Convert to string
					tags[k] = fmt.Sprintf("%v", v)
				}
			}
		}
	}

	// Extract fields based on fields mapping
	fields := make(map[string]interface{})
	if len(d.config.ParsedFieldsMapping) > 0 {
		// Use explicit mapping
		for recordField, influxField := range d.config.ParsedFieldsMapping {
			if value, ok := data[recordField]; ok {
				fields[influxField] = value
			}
		}
	} else {
		// Use all fields except those already used for tags, time, or measurement
		for field, value := range data {
			if field == d.config.TimeField ||
				field == d.config.MeasurementField ||
				field == d.config.TagsField {
				continue
			}
			fields[field] = value
		}
	}

	// Extract timestamp
	timestamp, err := d.getTimestamp(record, data)
	if err != nil {
		return nil, fmt.Errorf("failed to determine timestamp: %w", err)
	}

	// Create point
	point := influxdb2.NewPoint(
		measurement,
		tags,
		fields,
		timestamp,
	)

	return point, nil
}

// getStructuredData extracts structured data from the record.
func (d *Destination) getStructuredData(record opencdc.Record) (opencdc.StructuredData, error) {
	var data opencdc.StructuredData

	// Use the after data for create and update operations
	if record.Operation == opencdc.OperationCreate ||
		record.Operation == opencdc.OperationUpdate ||
		record.Operation == opencdc.OperationSnapshot {
		if record.Payload.After == nil {
			return nil, errors.New("record has no 'after' data")
		}

		if structData, ok := record.Payload.After.(opencdc.StructuredData); ok {
			data = structData
		} else if rawData, ok := record.Payload.After.(opencdc.RawData); ok {
			// Parse raw data as JSON
			var structData opencdc.StructuredData
			if err := json.Unmarshal(rawData, &structData); err != nil {
				return nil, fmt.Errorf("failed to unmarshal raw data: %w", err)
			}
			data = structData
		} else {
			return nil, fmt.Errorf("unsupported payload type: %T", record.Payload.After)
		}
	} else {
		return nil, fmt.Errorf("unsupported operation: %s", record.Operation)
	}

	return data, nil
}

// getMeasurement determines the measurement name for the record.
func (d *Destination) getMeasurement(record opencdc.Record, data opencdc.StructuredData) (string, error) {
	// If a static measurement value is provided, use it
	if d.config.MeasurementValue != "" {
		return d.config.MeasurementValue, nil
	}

	// If a measurement field is provided, extract it from the data
	if d.config.MeasurementField != "" {
		if measurementValue, ok := data[d.config.MeasurementField].(string); ok {
			return measurementValue, nil
		}
		// Try to convert to string
		if measurementValue, ok := data[d.config.MeasurementField]; ok {
			return fmt.Sprintf("%v", measurementValue), nil
		}
		return "", fmt.Errorf("measurement field '%s' not found in record data", d.config.MeasurementField)
	}

	// Fall back to the collection name from the metadata
	if collection, ok := record.Metadata[opencdc.MetadataCollection]; ok && collection != "" {
		return collection, nil
	}

	// If we still don't have a measurement name, error
	return "", errors.New("cannot determine measurement name, please provide 'measurementField' or 'measurementValue'")
}

// getTimestamp determines the timestamp for the record.
func (d *Destination) getTimestamp(record opencdc.Record, data opencdc.StructuredData) (time.Time, error) {
	// If time field is provided, extract it from the data
	if d.config.TimeField != "" {
		if timeValue, ok := data[d.config.TimeField].(time.Time); ok {
			return timeValue, nil
		}

		// Try to parse as string
		if timeStr, ok := data[d.config.TimeField].(string); ok {
			// Try to parse timestamp
			timestamp, err := source.ParseTimestamp(timeStr)
			if err == nil {
				return timestamp, nil
			}
		}

		// Try to parse as int (unix timestamp)
		if timeInt, ok := data[d.config.TimeField].(int64); ok {
			return time.Unix(0, timeInt), nil
		}
	}

	// Fall back to current time
	return time.Now(), nil
}
