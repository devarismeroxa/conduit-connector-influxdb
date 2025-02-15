// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package influxdb

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	DestinationConfigDestinationConfigParam = "destinationConfigParam"
	DestinationConfigGlobalConfigParamName  = "global_config_param_name"
)

func (DestinationConfig) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		DestinationConfigDestinationConfigParam: {
			Default:     "yes",
			Description: "DestinationConfigParam must be either yes or no (defaults to yes).",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationInclusion{List: []string{"yes", "no"}},
			},
		},
		DestinationConfigGlobalConfigParamName: {
			Default:     "",
			Description: "GlobalConfigParam is named global_config_param_name and needs to be\nprovided by the user.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
