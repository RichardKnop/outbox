package outbox

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
