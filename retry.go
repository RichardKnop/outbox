package outbox

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

type retryOptions struct {
	rand              *rand.Rand
	maxAttempts       int
	backoff           time.Duration
	backoffMultiplier int
	maxJitter         time.Duration
	isRetriable       func(error) bool
}

func (o *Outbox) retryOptions() retryOptions {
	return retryOptions{
		rand:              o.rand,
		maxAttempts:       o.retryAttempts,
		backoff:           o.retryBackoff,
		backoffMultiplier: o.retryBackoffMultiplier,
		maxJitter:         o.retryMaxJitter,
		isRetriable:       isSerializationError,
	}
}

// retry is a helper function that retries a function multiple times with exponential backoff.
func retry(ctx context.Context, opts retryOptions, f func() error) (err error) {
	backoff := opts.backoff
	for i := range opts.maxAttempts {
		if i == 0 {
			err = f()
			if err == nil {
				return nil
			}
		} else {
			if err != nil && !opts.isRetriable(err) {
				return fmt.Errorf("non-retriable error: %w", err)
			}
			backoffWithJitter := backoff
			if opts.maxJitter > 0 {
				backoffWithJitter += time.Duration(opts.rand.Int63n(int64(opts.maxJitter)))
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				err = f()
				if err == nil {
					return nil
				}
				backoff *= time.Duration(opts.backoffMultiplier)
			}
		}
	}
	return fmt.Errorf("after %d attempts, last error: %s", opts.maxAttempts, err)
}
