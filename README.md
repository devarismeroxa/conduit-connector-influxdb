# Conduit Connector InfluxDB

The InfluxDB connector is a [Conduit](https://github.com/ConduitIO/conduit) plugin that provides both source and destination connectors for [InfluxDB](https://docs.influxdata.com/influxdb3/core/).

## Source

The InfluxDB Source connector reads data from InfluxDB using the InfluxDB API. It can be configured to read data from a specific bucket and organization, and can filter data by measurement and time range.

The source connector supports reading data in batches and can be configured to read data incrementally based on a timestamp column.

### Configuration

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `url` | The URL of the InfluxDB instance. | http://localhost:8086 | Yes |
| `token` | The authentication token for the InfluxDB instance. | | Yes |
| `org` | The organization name. | | Yes |
| `bucket` | The bucket to read from. | | Yes |
| `measurement` | The measurement to read from. Leave empty to read from all measurements. | | No |
| `timeColumn` | The column to use for incremental reads based on time. It must be a timestamp column. | _time | Yes |
| `startTime` | The start time for the initial data fetch. Format should be RFC3339. | | No |
| `endTime` | The end time for the initial data fetch. Format should be RFC3339. Leave empty to use current time. | | No |
| `batchSize` | The number of records to read in each batch. | 1000 | No |

### Position

The source connector tracks its position using a timestamp to enable incremental reading. The position is stored as a JSON object with the following format:

```json
{
  "lastTimestamp": "2023-01-01T00:00:00Z"
}
```

### Example Configuration

```yaml
connector:
  id: influxdb-source
  type: source
  plugin: influxdb
  settings:
    url: http://localhost:8086
    token: myToken
    org: myOrg
    bucket: myBucket
    measurement: cpu
    timeColumn: _time
    startTime: "2023-01-01T00:00:00Z"
    batchSize: 1000
```

## Destination

The InfluxDB Destination connector writes data to InfluxDB. It can be configured to write to a specific bucket and organization.

The destination connector automatically maps Conduit records to InfluxDB points.

### Configuration

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `url` | The URL of the InfluxDB instance. | http://localhost:8086 | Yes |
| `token` | The authentication token for the InfluxDB instance. | | Yes |
| `org` | The organization name. | | Yes |
| `bucket` | The bucket to write to. | | Yes |
| `precision` | The precision of the timestamps. Valid values are ns, us, ms, s. | ns | No |
| `batchSize` | The number of records to write in each batch. | 1000 | No |
| `measurementField` | The field from the record to use as the measurement name. If not specified, the connector will use the collection name from the record metadata. | | No |
| `measurementValue` | The static measurement name to use. This takes precedence over measurementField. | | No |
| `tagsField` | The field from the record that contains tags. This should be a map of string to string. | | No |
| `fieldsMapping` | How to map record fields to InfluxDB fields. Format: "recordField1:influxField1,recordField2:influxField2". | | No |
| `timeField` | The field from the record to use as the timestamp. If not specified, the current time will be used. | _time | No |

### Record Mapping

Records are mapped to InfluxDB points as follows:

1. **Measurement**: The measurement name is determined in the following order:
   - If `measurementValue` is provided, it's used as the measurement name.
   - If `measurementField` is provided, the value of that field in the record is used.
   - Otherwise, the collection name from the record metadata is used.

2. **Tags**: If `tagsField` is provided, the value of that field in the record is used as tags. It should be a map of string to string.

3. **Fields**: If `fieldsMapping` is provided, the fields are mapped according to the mapping. Otherwise, all fields in the record except those used for tags, time, or measurement are used as fields.

4. **Timestamp**: If `timeField` is provided, the value of that field in the record is used as the timestamp. Otherwise, the current time is used.

### Example Configuration

```yaml
connector:
  id: influxdb-destination
  type: destination
  plugin: influxdb
  settings:
    url: http://localhost:8086
    token: myToken
    org: myOrg
    bucket: myBucket
    precision: ns
    batchSize: 1000
    measurementValue: cpu
    tagsField: tags
    timeField: time
```

## Usage in InfluxDB 3.0

This connector should work with both InfluxDB 2.x and 3.0. For InfluxDB 3.0, make sure to use the appropriate URL, token, organization, and bucket values according to the InfluxDB 3.0 documentation.

## Development

### Requirements

- [Go](https://golang.org/) 1.21 or later

### Building

```shell
go build -o conduit-connector-influxdb cmd/connector/main.go
```

### Testing

To run tests, you need a running InfluxDB instance. You can use Docker to start one:

```shell
docker run -p 8086:8086 -e INFLUXDB_ADMIN_USER=admin -e INFLUXDB_ADMIN_PASSWORD=password influxdb:2.7
```

Then, run the tests:

```shell
go test ./...
```

## License

This connector is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.