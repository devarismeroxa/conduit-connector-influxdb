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
	"github.com/influxdata/influxdb-client-go/v2/api/query"

	"github.com/devarismeroxa/conduit-connector-influxdb/source"
)

// Source errors definition.
var (
	ErrNilRecordFromInfluxDB = errors.New("received nil record from InfluxDB")
	ErrFailedGetTimestamp    = errors.New("failed to get timestamp from record")
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
func (s *Source) Open(_ context.Context, pos opencdc.Position) error {
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

// executeQuery runs the query against InfluxDB and returns the result.
func (s *Source) executeQuery(ctx context.Context) (*api.QueryTableResult, error) {
	query := s.buildQuery()
	result, err := s.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	return result, nil
}

// processRow handles the result set when there are no rows or an error occurred.
func (s *Source) processNoResults(result *api.QueryTableResult) error {
	// Check if there was an error during query execution
	if result.Err() != nil {
		return fmt.Errorf("error while reading query results: %w", result.Err())
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

	return sdk.ErrBackoffRetry
}

// buildRecord creates a Conduit record from an InfluxDB result row.
func (s *Source) buildRecord(influxRecord *query.FluxRecord) (opencdc.Record, error) {
	// Extract timestamp from the record
	timestamp, ok := influxRecord.ValueByKey(s.config.TimeColumn).(time.Time)
	if !ok {
		return opencdc.Record{}, ErrFailedGetTimestamp
	}

	// Build structured data from InfluxDB record
	data := make(opencdc.StructuredData)
	for k, v := range influxRecord.Values() {
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
		opencdc.MetadataCollection: influxRecord.Measurement(),
	}

	// Create key using the measurement and timestamp
	key := opencdc.StructuredData{
		"measurement": influxRecord.Measurement(),
		"time":        timestamp,
	}

	// Determine operation type (snapshot or create)
	operation := opencdc.OperationCreate
	if s.isSnapshot {
		operation = opencdc.OperationSnapshot
	}

	// Create record
	return opencdc.Record{
		Position:  posBytes,
		Operation: operation,
		Metadata:  metadata,
		Key:       key,
		Payload: opencdc.Change{
			After: data,
		},
	}, nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	// Execute the query
	result, err := s.executeQuery(ctx)
	if err != nil {
		return opencdc.Record{}, err
	}
	defer result.Close()

	// Check if there are any results
	if !result.Next() {
		err := s.processNoResults(result)
		return opencdc.Record{}, err
	}

	// Get record from the result
	influxRecord := result.Record()
	if influxRecord == nil {
		return opencdc.Record{}, ErrNilRecordFromInfluxDB
	}

	// Build and return the record
	return s.buildRecord(influxRecord)
}

// Ack acknowledges a record has been processed.
func (s *Source) Ack(_ context.Context, _ opencdc.Position) error {
	// Nothing to do here for InfluxDB source
	return nil
}

// Teardown closes the connection to InfluxDB.
func (s *Source) Teardown(_ context.Context) error {
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
	switch {
	case !s.position.LastTimestamp.IsZero():
		// If we have a last timestamp, use it as the start
		query += fmt.Sprintf(` |> range(start: %s`, s.position.LastTimestamp.Format(time.RFC3339))
	case !s.config.ParsedStartTime.IsZero():
		// Otherwise use configured start time
		query += fmt.Sprintf(` |> range(start: %s`, s.config.ParsedStartTime.Format(time.RFC3339))
	default:
		// If no start time is configured, use all data
		query += ` |> range(start: 0`
	}

	// Add end time if configured
	if !s.config.ParsedEndTime.IsZero() {
		query += fmt.Sprintf(`, stop: %s)`, s.config.ParsedEndTime.Format(time.RFC3339))
	} else {
		query += `)`
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
