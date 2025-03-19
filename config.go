package influxdb

// Config contains shared config parameters, common to the source and
// destination. If no shared parameters are needed, this can be removed.
type Config struct {
	// This struct is intentionally empty as we're using specific config
	// structs in the source and destination packages.
}
