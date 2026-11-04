package hook

import (
	"github.com/maxid/gmqx/internal/mqtt5"
	"github.com/maxid/gnety/codec/mqtt"
)

// WithUserProperty appends one MQTT 5 User Property to a raw PUBLISH property
// block. The block is the value of PublishEvent.Properties or
// ClusterMessage.Props; empty means "no properties".
//
// The codec validates UTF-8 and property constraints, so malformed input or a
// property the MQTT 5 PUBLISH rule rejects is returned as an error rather than
// silently dropped. MQTT 3.1.1 has no property block; callers must not add user
// properties to a 3.1.1 event.
func WithUserProperty(properties []byte, key, value string) ([]byte, error) {
	props, err := mqtt5.DecodePropertyBlock(properties)
	if err != nil {
		return nil, err
	}
	props.UserProperties = append(props.UserProperties, mqtt.UserProperty{Key: key, Value: value})
	return mqtt5.EncodePropertyBlock(props)
}

// UserProperties returns every value of key in the raw MQTT 5 property block.
// A missing key returns an empty slice; a malformed block returns an error.
func UserProperties(properties []byte, key string) ([]string, error) {
	props, err := mqtt5.DecodePropertyBlock(properties)
	if err != nil {
		return nil, err
	}
	var values []string
	for _, property := range props.UserProperties {
		if property.Key == key {
			values = append(values, property.Value)
		}
	}
	return values, nil
}
