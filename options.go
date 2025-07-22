package outbox

import (
	"time"
)

// Option is a function that can be used to configure the Outbox instance.
type Option func(*Outbox)

// WithLogger allows setting a custom logger for the Outbox instance.
func WithLogger(logger Logger) Option {
	return func(o *Outbox) {
		o.logger = logger
	}
}

// WithFlushLimit allows setting a custom flush limit for the Outbox instance.
func WithFlushLimit(limit int) Option {
	return func(o *Outbox) {
		o.flushLimit = limit
	}
}

// WithRetryAttempts allows setting a custom number of retry attempts for the Outbox instance.
func WithRetryAttempts(attempts int) Option {
	return func(o *Outbox) {
		o.retryAttempts = attempts
	}
}

// WithRetryBackoff allows setting a custom backoff duration for retries in the Outbox instance.
func WithRetryBackoff(backoff time.Duration) Option {
	return func(o *Outbox) {
		o.retryBackoff = backoff
	}
}

// WithRetryBackoffMultiplier allows setting a custom multiplier for the backoff duration in the Outbox instance.
func WithRetryBackoffMultiplier(multiplier int) Option {
	return func(o *Outbox) {
		o.retryBackoffMultiplier = multiplier
	}
}

// WithRetryMaxJitter allows setting a custom maximum jitter for retries in the Outbox instance.
func WithRetryMaxJitter(maxJitter time.Duration) Option {
	return func(o *Outbox) {
		o.retryMaxJitter = maxJitter
	}
}
