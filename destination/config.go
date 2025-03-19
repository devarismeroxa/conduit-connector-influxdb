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

package destination

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Config holds the configuration for the InfluxDB destination connector.
type Config struct {
	sdk.DefaultDestinationMiddleware

	// URL is the URL of the InfluxDB instance.
	URL string `json:"url" validate:"required" default:"http://localhost:8086"`
	// Token is the authentication token for the InfluxDB instance.
	Token string `json:"token" validate:"required"`
	// Org is the organization name.
	Org string `json:"org" validate:"required"`
	// Bucket is the bucket to write to.
	Bucket string `json:"bucket" validate:"required"`
	// Precision is the precision of the timestamps. Valid values are ns, us, ms, s.
	Precision string `json:"precision" validate:"inclusion=ns,us,ms,s" default:"ns"`
	// BatchSize is the number of records to write in each batch.
	BatchSize int `json:"batchSize" validate:"greater_than=0" default:"1000"`
	// MeasurementField is the field from the record to use as the measurement name.
	MeasurementField string `json:"measurementField"`
	// MeasurementValue is the static measurement name to use.
	MeasurementValue string `json:"measurementValue"`
	// TagsField is the field from the record that contains tags.
	TagsField string `json:"tagsField"`
	// FieldsMapping is a mapping from record fields to InfluxDB fields.
	FieldsMapping string `json:"fieldsMapping"`
	// TimeField is the field from the record to use as the timestamp.
	TimeField string `json:"timeField" default:"_time"`

	// parsedFieldsMapping holds the parsed fields mapping.
	ParsedFieldsMapping map[string]string
}

// Validate validates the configuration.
func (c *Config) Validate(ctx context.Context) error {
	_, err := url.Parse(c.URL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	if c.Token == "" {
		return errors.New("token is required")
	}

	if c.Org == "" {
		return errors.New("org is required")
	}

	if c.Bucket == "" {
		return errors.New("bucket is required")
	}

	switch c.Precision {
	case "ns", "us", "ms", "s":
		// Valid precision
	default:
		return fmt.Errorf("invalid precision: %s, must be one of: ns, us, ms, s", c.Precision)
	}

	if c.BatchSize <= 0 {
		return errors.New("batchSize must be greater than 0")
	}

	// Parse fields mapping if provided
	if c.FieldsMapping != "" {
		c.ParsedFieldsMapping = make(map[string]string)
		mappings := strings.Split(c.FieldsMapping, ",")
		for _, mapping := range mappings {
			parts := strings.Split(mapping, ":")
			if len(parts) != 2 {
				return fmt.Errorf("invalid fields mapping format: %s, expected format 'recordField:influxField'", mapping)
			}
			recordField := strings.TrimSpace(parts[0])
			influxField := strings.TrimSpace(parts[1])
			if recordField == "" || influxField == "" {
				return fmt.Errorf("invalid fields mapping: %s, both record field and InfluxDB field must be non-empty", mapping)
			}
			c.ParsedFieldsMapping[recordField] = influxField
		}
	}

	err = c.DefaultDestinationMiddleware.Validate(ctx)
	if err != nil {
		return fmt.Errorf("middleware validation failed: %w", err)
	}

	return nil
}
