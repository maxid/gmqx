package gmqx

import "github.com/maxid/gmqx/hook"

// WithUserProperty forwards to hook.WithUserProperty.
func WithUserProperty(properties []byte, key, value string) ([]byte, error) {
	return hook.WithUserProperty(properties, key, value)
}

// UserProperties forwards to hook.UserProperties.
func UserProperties(properties []byte, key string) ([]string, error) {
	return hook.UserProperties(properties, key)
}
