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
	sdk "github.com/conduitio/conduit-connector-sdk"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/devarismeroxa/conduit-connector-influxdb/destination"
	"github.com/devarismeroxa/conduit-connector-influxdb/source"
)

var (
	ErrNoAfterData                = errors.New("record has no 'after' data")
	ErrUnsupportedOperation       = errors.New("unsupported operation")
	ErrMeasurementFieldNotFound   = errors.New("measurement field not found in record data")
	ErrCannotDetermineMeasurement = errors.New("cannot determine measurement name, please provide 'measurementField' or 'measurementValue'")
	ErrInvalidPrecision           = errors.New("invalid precision, must be one of: ns, us, ms, s")
	ErrInvalidFieldsMapping       = errors.New("invalid fields mapping format, expected format 'recordField:influxField'")
	ErrEmptyFieldsMapping         = errors.New("invalid fields mapping: both record field and InfluxDB field must be non-empty")
	ErrUnsupportedPayloadType     = errors.New("unsupported payload type")
	ErrNegativeBatchSize          = errors.New("BatchSize must be non-negative")
)
// Destination is an implementation of sdk.Destination that writes to InfluxDB.
type Destination struct {
	sdk.UnimplementedDestination

	config   destination.Config
	client   influxdb2.Client
	writeAPI api.WriteAPI
}

// NewDestination creates a new instance of the InfluxDB destination connector.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

// Parameters returns the required parameters for the destination connector.
func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

func (d *Destination) Open(ctx context.Context) error {
	// Initialize client
	options := influxdb2.DefaultOptions()
	// Check for negative BatchSize to prevent integer overflow
	if d.config.BatchSize < 0 {
		return ErrNegativeBatchSize
	}
	const maxBatchSize uint = 1000000
	if d.config.BatchSize > 0 {
		batchSize := d.config.BatchSize
		if batchSize > int(maxBatchSize) {
			options.SetBatchSize(maxBatchSize)
		} else if batchSize > 0 {
			// Safe conversion as we've checked the upper and lower bounds
			options.SetBatchSize(uint(batchSize))
		}
	}
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
func (d *Destination) Teardown(_ context.Context) error {
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
	tags, err := d.extractTags(data)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tags: %w", err)
	}

	// Extract fields
	fields := d.extractFields(data)

	// Extract timestamp
	timestamp := d.getTimestamp(record, data)

	// Build and return point
	return d.buildPoint(measurement, tags, fields, timestamp), nil
}

// extractTags extracts tag values from the structured data.
func (d *Destination) extractTags(data opencdc.StructuredData) (map[string]string, error) {
	tags := make(map[string]string)
	if d.config.TagsField == "" {
		return tags, nil
	}

	tagsData, ok := data[d.config.TagsField]
	if !ok {
		return tags, nil
	}

	// Try different types of tag data
	if typedTags, ok := tagsData.(map[string]string); ok {
		return typedTags, nil
	}
	return d.convertToStringMap(tagsData)
}

// convertToStringMap converts a map-like object to a string map for tags.
func (d *Destination) convertToStringMap(data interface{}) (map[string]string, error) {
	result := make(map[string]string)
	if tagsData, ok := data.(map[string]interface{}); ok {
		for k, v := range tagsData {
			if stringVal, ok := v.(string); ok {
				result[k] = stringVal
			} else {
				result[k] = fmt.Sprintf("%v", v)
			}
		}
		return result, nil
	}
	
	if tagsData, ok := data.(opencdc.StructuredData); ok {
		for k, v := range tagsData {
			if stringVal, ok := v.(string); ok {
				result[k] = stringVal
			} else {
				result[k] = fmt.Sprintf("%v", v)
			}
		}
		return result, nil
	}
	
	// If we can't convert the data, return an empty map
	return result, nil
}

// extractFields extracts field values from the structured data.
func (d *Destination) extractFields(data opencdc.StructuredData) map[string]interface{} {
	fields := make(map[string]interface{})
	
	if len(d.config.ParsedFieldsMapping) > 0 {
		// Use explicit mapping
		for recordField, influxField := range d.config.ParsedFieldsMapping {
			if value, ok := data[recordField]; ok {
				fields[influxField] = value
			}
		}
		return fields
	}
	
	// Use all fields except those already used for tags, time, or measurement
	for field, value := range data {
		if field == d.config.TimeField ||
			field == d.config.MeasurementField ||
			field == d.config.TagsField {
			continue
		}
		fields[field] = value
	}
	
	return fields
}

// buildPoint creates an InfluxDB point with the provided data.
func (d *Destination) buildPoint(measurement string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) *write.Point {
	return influxdb2.NewPoint(
		measurement,
		tags,
		fields,
		timestamp,
	)
}

// getStructuredData extracts structured data from the record.
func (d *Destination) getStructuredData(record opencdc.Record) (opencdc.StructuredData, error) {
	var data opencdc.StructuredData

	// Check operation type first
	switch record.Operation {
	case opencdc.OperationCreate, opencdc.OperationUpdate, opencdc.OperationSnapshot:
		if record.Payload.After == nil {
			return nil, ErrNoAfterData
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
			return nil, fmt.Errorf("%w: %T", ErrUnsupportedPayloadType, record.Payload.After)
		}
	case opencdc.OperationDelete:
		return nil, fmt.Errorf("%w: delete operations not supported by InfluxDB", ErrUnsupportedOperation)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedOperation, record.Operation)
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
		return "", fmt.Errorf("%w: %s", ErrMeasurementFieldNotFound, d.config.MeasurementField)
	}

	// Fall back to the collection name from the metadata
	if collection, ok := record.Metadata[opencdc.MetadataCollection]; ok && collection != "" {
		return collection, nil
	}

	// If we still don't have a measurement name, error
	return "", ErrCannotDetermineMeasurement
}

// getTimestamp determines the timestamp for the record.
func (d *Destination) getTimestamp(_ opencdc.Record, data opencdc.StructuredData) time.Time {
	// If no time field is provided, use current time
	if d.config.TimeField == "" {
		return time.Now()
	}
	
	// Get the time field value
	timeValue, ok := data[d.config.TimeField]
	if !ok {
		return time.Now()
	}

	// Try as time.Time
	if timestamp, ok := timeValue.(time.Time); ok {
		return timestamp
	}

	// Try as string
	if timeStr, ok := timeValue.(string); ok {
		timestamp, err := source.ParseTimestamp(timeStr)
		if err == nil {
			return timestamp
		}
	}

	// Try as int64 (unix timestamp)
	if timeInt, ok := timeValue.(int64); ok {
		return time.Unix(0, timeInt)
	}

	// Fall back to current time
	return time.Now()
}
