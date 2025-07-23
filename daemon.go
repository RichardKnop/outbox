package outbox

import (
	"context"
	"errors"
	"sync"
	"time"
)

type clock func() time.Time

// DaemonOption is a function that can be used to configure the Daemon instance.
type DaemonOption func(*Daemon)

// WithDaemonInterval allows setting a custom interval for the Daemon instance.
func WithDaemonInterval(interval time.Duration) DaemonOption {
	return func(d *Daemon) {
		d.interval = interval
	}
}

// WithDaemonMaxJitter allows setting a custom maximum jitter for the Daemon instance.
func WithDaemonMaxJitter(maxJitter time.Duration) DaemonOption {
	return func(d *Daemon) {
		d.maxJitter = maxJitter
	}
}

// WithDaemonDeleteAfter allows setting a custom duration after which messages will be deleted from the outbox.
func WithDaemonDeleteAfter(deleteAfter time.Duration) DaemonOption {
	return func(d *Daemon) {
		d.deleteAfter = deleteAfter
	}
}

// Daemon is a struct that manages the periodic flushing of the outbox messages.
// It runs in a separate goroutine and flushes messages at regular intervals, with optional jitter
type Daemon struct {
	outbox      *Outbox
	interval    time.Duration
	maxJitter   time.Duration
	deleteAfter time.Duration
	now         clock
	wg          *sync.WaitGroup
}

const (
	defaultDaemonInterval    = 10 * time.Second
	defaultDaemonMaxJitter   = 100 * time.Millisecond
	defaultDaemonDeleteAfter = 1 * time.Hour
)

// NewDaemon creates a new Daemon instance that will flush the outbox messages periodically.
// The interval specifies how often the flush should occur, and maxJitter adds a random delay
// to the flush interval to avoid thundering herd problems.
// The jitter is a random duration between 0 and maxJitter that is added to the flush interval.
func NewDaemon(outbox *Outbox, opts ...DaemonOption) *Daemon {
	return &Daemon{
		outbox:      outbox,
		interval:    defaultDaemonInterval,
		maxJitter:   defaultDaemonMaxJitter,
		deleteAfter: defaultDaemonDeleteAfter,
		now:         time.Now,
	}
}

// Start begins the daemon's operation, flushing the outbox at the specified intervals.
// The daemon will stop flushing when the context is canceled.
func (d *Daemon) Start(ctx context.Context) {
	ticker := time.NewTicker(d.interval - d.maxJitter/2)

	d.wg = new(sync.WaitGroup)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if d.maxJitter > 0 {
					jitterDuration := time.Duration(d.outbox.rand.Int63n(int64(d.maxJitter)))
					if err := jitter(ctx, jitterDuration); err != nil {
						if !errors.Is(err, context.Canceled) {
							d.outbox.logger.Println("random jitter failed:", err.Error())
						}
						return
					}
				}

				d.flush(ctx)
				d.cleanup(ctx)
			}
		}
	}()
}

// Stop stops the daemon and waits for it to finish any ongoing operations.
func (d *Daemon) Stop() {
	d.wg.Wait()
}

func (d *Daemon) flush(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := d.outbox.Flush(ctx); err != nil {
		d.outbox.logger.Println("regular flush failed:", err.Error())
	}
}

func (d *Daemon) cleanup(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := d.outbox.Cleanup(ctx, d.now().Add(d.deleteAfter)); err != nil {
		d.outbox.logger.Println("regular cleanup failed:", err.Error())
	}
}

func jitter(ctx context.Context, jitterDuration time.Duration) error {
	select {
	case <-time.After(jitterDuration):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
