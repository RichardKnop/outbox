package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

// Producer interface defines the methods required for sending messages to a destination.
// It is used to flush the outbox messages to external systems such as Kafka or email services.
type Producer interface {
	Send(ctx context.Context, topic string, key, val []byte) error
}

type Logger interface {
	Print(v ...any)
	Printf(format string, v ...any)
	Println(v ...any)
}

// Outbox is a struct that manages the outbox pattern, allowing messages to be stored and sent later.
type Outbox struct {
	table      string
	db         *sql.DB
	producer   Producer
	logger     Logger
	flushLimit int
	txOptions  *sql.TxOptions
	rand       *rand.Rand
}

const (
	defaultFlushLimit             = 100
	defaultRetryAttempts          = 3
	defaultRetryBackoff           = 100 * time.Millisecond
	defaultRetryBackoffMultiplier = 3
	defaultRetryMaxJitter         = 50 * time.Millisecond
)

// New creates a new Outbox instance with the specified table name, database connection, and producer.
// It also accepts optional configuration functions to customize the Outbox instance.
func New(tableName string, db *sql.DB, producer Producer, opts ...Option) *Outbox {
	outbox := Outbox{
		table:      tableName,
		db:         db,
		producer:   producer,
		logger:     log.New(os.Stdout, "[Outbox]:", log.LstdFlags),
		flushLimit: defaultFlushLimit,
		txOptions: &sql.TxOptions{
			Isolation: sql.LevelReadCommitted,
		},
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	for _, o := range opts {
		o(&outbox)
	}

	return &outbox
}

// Message represents a message to be sent through the outbox.
// It contains the destination (e.g., Kafka topic), key, and value of the message
type Message struct {
	Destination string // kafka topic or other destination (email address etc)
	Key         []byte
	Value       []byte
}

// Send adds a message (or multiple messages) to the outbox table.
func (o *Outbox) Send(ctx context.Context, tx *sql.Tx, messages ...Message) error {
	if len(messages) == 0 {
		return nil
	}

	return o.sendMessages(ctx, tx, messages...)
}

func (o *Outbox) sendMessages(ctx context.Context, tx *sql.Tx, messages ...Message) error {
	query := fmt.Sprintf(`insert into "%s" ("destination", "key", "value") values `, o.table)
	placeholderIdx := 1
	for i := range messages {
		query += fmt.Sprintf("($%d, $%d, $%d)", placeholderIdx, placeholderIdx+1, placeholderIdx+2)
		placeholderIdx += 3
		if i < len(messages)-1 {
			query += ", "
		}
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("prepare statement failed: %w", err)
	}
	defer stmt.Close()

	args := make([]any, 0, len(messages)*3)
	for _, msg := range messages {
		args = append(args, msg.Destination, msg.Key, msg.Value)
	}
	_, err = stmt.ExecContext(ctx, args...)
	if err != nil {
		return fmt.Errorf("execute statement failed: %w", err)
	}

	return nil
}

// Flush processes all messages in the outbox, sending them to the configured producer.
// It will continue to flush messages until there are no more messages to process.
// This method is typically called after sending messages to ensure they are processed and sent to their destination.
// Usually, you would call this method in a separate goroutine to avoid blocking the main application flow.
func (o *Outbox) Flush(ctx context.Context) error {
	for {
		potentiallyMoreMessages, err := o.flushMessages(ctx)
		if err != nil {
			return fmt.Errorf("flush messages failed: %w", err)
		}
		if !potentiallyMoreMessages {
			break
		}
	}
	return nil
}

func (o *Outbox) flushMessages(ctx context.Context) (bool, error) {
	var (
		potentiallyMoreMessages bool
		limit                   = o.flushLimit
	)
	if err := transactional(ctx, o.db, o.txOptions, func(ctx context.Context, tx *sql.Tx) error {
		messages, err := o.selectMessages(ctx, tx, limit)
		if err != nil {
			return fmt.Errorf("select messages for producer failed: %w", err)
		}

		for _, msg := range messages {
			if err := o.producer.Send(ctx, msg.Destination, msg.Key, msg.Value); err != nil {
				return fmt.Errorf("producer send message failed: %w", err)
			}
		}

		// If we selected fewer messages than the limit, it means there are no more messages to process.
		// If we selected exactly the limit, it means there might be more messages to process
		potentiallyMoreMessages = len(messages) == limit

		return nil
	}); err != nil {
		return false, fmt.Errorf("flush messages failed: %w", err)
	}

	return potentiallyMoreMessages, nil
}

// Cleanup removes messages from the outbox that have been processed.
// It deletes messages that have a non-null processed_at timestamp and are older than the specified time.
func (o *Outbox) Cleanup(ctx context.Context, olderThan time.Time) error {
	if err := transactional(ctx, o.db, o.txOptions, func(ctx context.Context, tx *sql.Tx) error {
		stmt, err := tx.Prepare(fmt.Sprintf(`
			delete from "%s" where processed_at is not null and created_at < $1`, o.table))
		if err != nil {
			return fmt.Errorf("prepare statement failed: %w", err)
		}
		defer stmt.Close()

		_, err = stmt.ExecContext(ctx, olderThan)
		if err != nil {
			return fmt.Errorf("delete statement failed: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("delete processed messages failed: %w", err)
	}
	return nil
}

// selectMessages retrieves messages from the outbox table that have not been processed yet.
// It updates the processed_at timestamp to mark them as processed.
// The messages are returned in the order of their IDs, and the number of messages returned
// is limited by the provided limit parameter.
func (o *Outbox) selectMessages(ctx context.Context, tx *sql.Tx, limit int) ([]Message, error) {
	stmt, err := tx.Prepare(fmt.Sprintf(`
		update "%s" set processed_at = now() where id in (
			select id from "%s" where processed_at is null order by id asc limit $1 for update
		) returning destination, key, value`, o.table, o.table))
	if err != nil {
		return nil, fmt.Errorf("prepare statement failed: %w", err)
	}
	defer stmt.Close()

	var messages []Message
	rows, err := stmt.QueryContext(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("select statement failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.Destination, &msg.Key, &msg.Value); err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration failed: %w", err)
	}

	return messages, nil
}
