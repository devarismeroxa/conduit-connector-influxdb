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

package source

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Common errors for the InfluxDB source connector
var (
	ErrTokenRequired         = errors.New("token is required")
	ErrOrgRequired           = errors.New("org is required")
	ErrBucketRequired        = errors.New("bucket is required")
	ErrTimeColumnRequired    = errors.New("timeColumn is required")
	ErrBatchSizeTooSmall     = errors.New("batchSize must be greater than 0")
	ErrStartTimeAfterEndTime = errors.New("startTime must be before endTime")
)

// errFailedToParseTimestamp returns an error for timestamp parsing failures
func errFailedToParseTimestamp(timestamp string) error {
	return fmt.Errorf("failed to parse timestamp: %s", timestamp)
}

// Config holds the configuration for the InfluxDB source connector.
type Config struct {
	sdk.DefaultSourceMiddleware

	// URL is the URL of the InfluxDB instance.
	URL string `json:"url" validate:"required" default:"http://localhost:8086"`
	// Token is the authentication token for the InfluxDB instance.
	Token string `json:"token" validate:"required"`
	// Org is the organization name.
	Org string `json:"org" validate:"required"`
	// Bucket is the bucket to read from.
	Bucket string `json:"bucket" validate:"required"`
	// Measurement is the measurement to read from. Leave empty to read from all measurements.
	Measurement string `json:"measurement"`
	// TimeColumn is the column to use for incremental reads based on time.
	TimeColumn string `json:"timeColumn" validate:"required" default:"_time"`
	// StartTime is the start time for the initial data fetch. Format should be RFC3339.
	StartTime string `json:"startTime"`
	// EndTime is the end time for the initial data fetch. Format should be RFC3339.
	EndTime string `json:"endTime"`
	// BatchSize is the number of records to read in each batch.
	BatchSize int `json:"batchSize" validate:"greater_than=0" default:"1000"`

	// ParsedStartTime and ParsedEndTime store the parsed time values
	ParsedStartTime time.Time
	ParsedEndTime   time.Time
}

// Validate validates the configuration.
func (c *Config) Validate(ctx context.Context) error {
	_, err := url.Parse(c.URL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	if c.Token == "" {
		return ErrTokenRequired
	}

	if c.Org == "" {
		return ErrOrgRequired
	}

	if c.Bucket == "" {
		return ErrBucketRequired
	}

	if c.TimeColumn == "" {
		return ErrTimeColumnRequired
	}

	if c.BatchSize <= 0 {
		return ErrBatchSizeTooSmall
	}

	// Parse start time if provided
	if c.StartTime != "" {
		startTime, err := time.Parse(time.RFC3339, c.StartTime)
		if err != nil {
			return fmt.Errorf("invalid startTime, should be RFC3339 format: %w", err)
		}
		c.ParsedStartTime = startTime
	}

	// Parse end time if provided
	if c.EndTime != "" {
		endTime, err := time.Parse(time.RFC3339, c.EndTime)
		if err != nil {
			return fmt.Errorf("invalid endTime, should be RFC3339 format: %w", err)
		}
		c.ParsedEndTime = endTime
	}

	if !c.ParsedStartTime.IsZero() && !c.ParsedEndTime.IsZero() && c.ParsedStartTime.After(c.ParsedEndTime) {
		return ErrStartTimeAfterEndTime
	}

	err = c.DefaultSourceMiddleware.Validate(ctx)
	if err != nil {
		return fmt.Errorf("middleware validation failed: %w", err)
	}

	return nil
}

// ParseTimestamp parses a timestamp from a string.
func ParseTimestamp(timestamp string) (time.Time, error) {
	// Try to parse as RFC3339
	t, err := time.Parse(time.RFC3339, timestamp)
	if err == nil {
		return t, nil
	}

	// Try to parse as Unix nanoseconds
	nanos, err := strconv.ParseInt(timestamp, 10, 64)
	if err == nil {
		return time.Unix(0, nanos), nil
	}

	return time.Time{}, errFailedToParseTimestamp(timestamp)
}
