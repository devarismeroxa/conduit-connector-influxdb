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
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/devarismeroxa/conduit-connector-influxdb/source"
)

// Source is an implementation of sdk.Source that reads from InfluxDB.
type Source struct {
	sdk.UnimplementedSource

	config   source.Config
	position position
	client   influxdb2.Client
	queryAPI api.QueryAPI

	// isSnapshot indicates if we are in snapshot mode
	isSnapshot bool
}

// position tracks the source's progress.
type position struct {
	// LastTimestamp is the timestamp of the last record read.
	LastTimestamp time.Time `json:"lastTimestamp,omitempty"`
}

// NewSource creates a new instance of the InfluxDB source connector.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{})
}

// Parameters returns the required parameters for the source connector.
func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

// Open initializes the connector and starts a connection to InfluxDB.
func (s *Source) Open(ctx context.Context, pos opencdc.Position) error {
	// Parse position if provided
	if len(pos) > 0 {
		var p position
		err := json.Unmarshal(pos, &p)
		if err != nil {
			return fmt.Errorf("failed to unmarshal position: %w", err)
		}
		s.position = p
	}

	// Initialize client
	s.client = influxdb2.NewClient(s.config.URL, s.config.Token)

	// Get query API
	s.queryAPI = s.client.QueryAPI(s.config.Org)

	// If the position is empty and startTime is set, use startTime as the starting position
	if s.position.LastTimestamp.IsZero() && !s.config.ParsedStartTime.IsZero() {
		s.position.LastTimestamp = s.config.ParsedStartTime
	}

	// If we still don't have a timestamp, we're starting a new snapshot
	if s.position.LastTimestamp.IsZero() {
		s.isSnapshot = true
	}

	return nil
}

// Read reads data from InfluxDB and returns it as a record.
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	// Build Flux query
	query := s.buildQuery()

	// Execute the query
	result, err := s.queryAPI.Query(ctx, query)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to execute query: %w", err)
	}
	defer result.Close()

	// Process results
	if !result.Next() {
		// No more results
		if result.Err() != nil {
			return opencdc.Record{}, fmt.Errorf("error while reading query results: %w", result.Err())
		}

		// If we're in snapshot mode, mark it as completed
		if s.isSnapshot {
			s.isSnapshot = false
			// If endTime was specified, use it as the new position
			if !s.config.ParsedEndTime.IsZero() {
				s.position.LastTimestamp = s.config.ParsedEndTime
			} else {
				// Otherwise use current time
				s.position.LastTimestamp = time.Now()
			}
		}

		// Return error to indicate no more records
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	// Get record and convert to structured data
	record := result.Record()
	if record == nil {
		return opencdc.Record{}, fmt.Errorf("received nil record from InfluxDB")
	}

	// Extract timestamp from the record
	timestamp, ok := record.ValueByKey(s.config.TimeColumn).(time.Time)
	if !ok {
		return opencdc.Record{}, fmt.Errorf("failed to get timestamp from record")
	}

	// Build structured data from InfluxDB record
	data := make(opencdc.StructuredData)
	for k, v := range record.Values() {
		data[k] = v
	}

	// Create and save position
	s.position.LastTimestamp = timestamp
	posBytes, err := json.Marshal(s.position)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to marshal position: %w", err)
	}

	// Create metadata
	metadata := opencdc.Metadata{
		opencdc.MetadataCollection: record.Measurement(),
	}

	// Create key using the measurement and timestamp
	key := opencdc.StructuredData{
		"measurement": record.Measurement(),
		"time":        timestamp,
	}

	// Create record with appropriate operation type
	if s.isSnapshot {
		return sdk.Util.Source.NewRecordSnapshot(
			posBytes,
			metadata,
			key,
			data,
		), nil
	}
	return sdk.Util.Source.NewRecordCreate(
		posBytes,
		metadata,
		key,
		data,
	), nil
}

// Ack acknowledges a record has been processed.
func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	// Nothing to do here for InfluxDB source
	return nil
}

// Teardown closes the connection to InfluxDB.
func (s *Source) Teardown(ctx context.Context) error {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
	return nil
}

// buildQuery constructs a Flux query based on the current position and configuration.
func (s *Source) buildQuery() string {
	var query string

	// Start with the bucket
	query = fmt.Sprintf(`from(bucket: "%s")`, s.config.Bucket)

	// Add time range
	if !s.position.LastTimestamp.IsZero() {
		// If we have a last timestamp, use it as the start
		query += fmt.Sprintf(` |> range(start: %s`, s.position.LastTimestamp.Format(time.RFC3339))
	} else if !s.config.ParsedStartTime.IsZero() {
		// Otherwise use configured start time
		query += fmt.Sprintf(` |> range(start: %s`, s.config.ParsedStartTime.Format(time.RFC3339))
	} else {
		// If no start time is configured, use all data
		query += ` |> range(start: 0`
	}

	// Add end time if configured
	// Add end time if configured
	if !s.config.ParsedEndTime.IsZero() {
		query += fmt.Sprintf(`, stop: %s)`, s.config.ParsedEndTime.Format(time.RFC3339))
	}

	// Filter by measurement if configured
	if s.config.Measurement != "" {
		query += fmt.Sprintf(` |> filter(fn: (r) => r._measurement == "%s")`, s.config.Measurement)
	}

	// Order by time for consistent results
	query += fmt.Sprintf(` |> sort(columns: ["%s"], desc: false)`, s.config.TimeColumn)

	// Limit the number of records to the batch size
	query += fmt.Sprintf(` |> limit(n: %d)`, s.config.BatchSize)

	return query
}
