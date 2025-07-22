package outbox

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testRetryOptions = retryOptions{
	rand:              rand.New(rand.NewSource(time.Now().UnixNano())),
	maxAttempts:       defaultRetryAttempts,
	backoff:           defaultRetryBackoff,
	backoffMultiplier: defaultRetryBackoffMultiplier,
	maxJitter:         defaultRetryMaxJitter,
	isRetriable: func(err error) bool {
		return strings.Contains(err.Error(), "simulated error")
	},
}

func TestRetry_NoErrors(t *testing.T) {
	t.Parallel()

	var (
		ctx      = context.Background()
		attempts int
	)

	err := retry(ctx, testRetryOptions, func() error {
		attempts++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, attempts, "should have succeeded on the first attempt")
}

func TestRetry_Errors(t *testing.T) {
	t.Parallel()

	var (
		ctx          = context.Background()
		retryOptions = testRetryOptions
		attempts     int
	)

	err := retry(ctx, retryOptions, func() error {
		attempts++
		if attempts < retryOptions.maxAttempts {
			return fmt.Errorf("simulated error")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, retryOptions.maxAttempts, attempts, "should have retried the expected number of times")
}
