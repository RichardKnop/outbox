//go:build it
// +build it

package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

//go:generate mockery --name=Producer --structname=MockProducer --inpackage --case=snake --testonly

const (
	host         = "localhost"
	port         = 5432
	testPassword = "secret"
	testTable    = "outbox_test"
	dbTimeout    = 5 * time.Second
)

func TestOutboxTestSuite(t *testing.T) {
	suite.Run(t, new(OutboxTestSuite))
}

// OutboxTestSuite is a test suite to run integration tests for the access store.
type OutboxTestSuite struct {
	suite.Suite
	db             *sql.DB
	producer       *MockProducer
	outbox         *Outbox
	dockerPool     *dockertest.Pool
	dockerResource *dockertest.Resource
}

func psqlInfo(resource *dockertest.Resource) string {
	return fmt.Sprintf("postgres://postgres:secret@localhost:%s/postgres?sslmode=disable", resource.GetPort("5432/tcp"))
}

func postgresConfig() []string {
	return []string{
		fmt.Sprintf("POSTGRES_PASSWORD=%v", testPassword),
		"listen_addresses = '*'",
	}
}

// SetupSuite will run before the tests in the suite are run.
func (s *OutboxTestSuite) SetupSuite() {
	var err error
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	s.dockerPool, err = dockertest.NewPool("")
	s.Require().NoError(err, "Failed to create docker pool")

	// uses pool to try to connect to Docker
	err = s.dockerPool.Client.Ping()
	s.Require().NoError(err, "Could not connect to Docker")

	// pulls an image, creates a container based on it and runs it
	s.dockerResource, err = s.dockerPool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "17.5-alpine3.22",
		Env:        postgresConfig(),
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	s.Require().NoError(err, "Could not start resource")
	// Tell docker to hard kill the container in 60 seconds
	s.dockerResource.Expire(60)

	connectionString := psqlInfo(s.dockerResource)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	err = s.dockerPool.Retry(func() error {
		var err error
		s.db, err = sql.Open("postgres", connectionString)
		if err != nil {
			return err
		}
		return s.db.Ping()
	})
	s.Require().NoError(err, "Could not connect to database")

	s.migrateDown()
	s.migrateUp()
}

// TearDownSuite will run after all the tests in the suite have been run.
func (s *OutboxTestSuite) TearDownSuite() {
	s.db.Close()
	err := s.dockerPool.Purge(s.dockerResource)
	s.Require().NoError(err, "Could not purge resource")
}

// SetupTest will run before each test in the suite.
func (s *OutboxTestSuite) SetupTest() {
	s.producer = new(MockProducer)
	s.outbox = New(testTable, s.db, s.producer)
	s.cleanup()
	s.assertEmptyOutbox()
}

// TearDownTest will run after each test in the suite.
func (s *OutboxTestSuite) TearDownTest() {
	s.producer.AssertExpectations(s.T())
}

func (s *OutboxTestSuite) TestFlushMessage_ProducerError() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	msg := Message{
		Destination: "test-topic",
		Key:         []byte("test-key"),
		Value:       []byte("test-value"),
	}

	err := transactional(ctx, s.db, s.outbox.txOptions, func(ctx context.Context, tx *sql.Tx) error {
		return s.outbox.Send(ctx, tx, msg)
	})
	s.Require().NoError(err)
	s.assertOutboxMessages(msg)

	s.producer.On("Send", mock.Anything, msg.Destination, msg.Key, msg.Value).Return(fmt.Errorf("producer error")).Once()

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.outbox.Flush(context.Background())
		s.Require().Error(err)
	}()
	wg.Wait()
	s.assertOutboxMessages(msg)
}

func (s *OutboxTestSuite) TestFlushMessage_SingleMessage() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	msg := Message{
		Destination: "test-topic",
		Key:         []byte("test-key"),
		Value:       []byte("test-value"),
	}

	err := transactional(ctx, s.db, s.outbox.txOptions, func(ctx context.Context, tx *sql.Tx) error {
		return s.outbox.Send(ctx, tx, msg)
	})
	s.Require().NoError(err)
	s.assertOutboxMessages(msg)

	s.producer.On("Send", mock.Anything, msg.Destination, msg.Key, msg.Value).Return(nil).Once()

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.outbox.Flush(context.Background())
		s.Require().NoError(err)
	}()
	wg.Wait()
	s.assertWholeOutboxProcessed()
}

func (s *OutboxTestSuite) TestFlushMessage_ManyMessages() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	messages := mockMessages(101)

	err := transactional(ctx, s.db, s.outbox.txOptions, func(ctx context.Context, tx *sql.Tx) error {
		return s.outbox.Send(ctx, tx, messages...)
	})
	s.Require().NoError(err)
	s.assertOutboxMessages(messages...)

	for _, msg := range messages {
		s.producer.On("Send", mock.Anything, msg.Destination, msg.Key, msg.Value).Return(nil).Once()
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.outbox.Flush(context.Background())
		s.Require().NoError(err)
	}()
	wg.Wait()
	s.assertWholeOutboxProcessed()
}

func (s *OutboxTestSuite) TestFlushMessage_ConcurrentFlushing() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	messages := mockMessages(201)

	err := transactional(ctx, s.db, s.outbox.txOptions, func(ctx context.Context, tx *sql.Tx) error {
		return s.outbox.Send(ctx, tx, messages...)
	})
	s.Require().NoError(err)
	s.assertOutboxMessages(messages...)

	for _, msg := range messages {
		s.producer.On("Send", mock.Anything, msg.Destination, msg.Key, msg.Value).Return(nil).Once()
	}

	wg := new(sync.WaitGroup)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.outbox.Flush(context.Background())
			s.Require().NoError(err)
		}()
	}
	wg.Wait()
	s.assertWholeOutboxProcessed()
}

func (s *OutboxTestSuite) TestCleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	messages := mockMessages(10)

	err := transactional(ctx, s.db, s.outbox.txOptions, func(ctx context.Context, tx *sql.Tx) error {
		return s.outbox.Send(ctx, tx, messages...)
	})
	s.Require().NoError(err)
	s.assertOutboxMessages(messages...)

	// No messages should be processed yet, therefor cleanup should not remove any messages
	err = s.outbox.Cleanup(ctx, time.Now())
	s.Require().NoError(err)
	s.assertOutboxMessages(messages...)

	// Let's flush the outbox now to process the messages
	for _, msg := range messages {
		s.producer.On("Send", mock.Anything, msg.Destination, msg.Key, msg.Value).Return(nil).Once()
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.outbox.Flush(context.Background())
		s.Require().NoError(err)
	}()
	wg.Wait()
	s.assertWholeOutboxProcessed()

	// Now we can cleanup the processed messages and outbox should be empty
	err = s.outbox.Cleanup(ctx, time.Now())
	s.Require().NoError(err)
	s.assertEmptyOutbox()
}

func (s *OutboxTestSuite) migrateUp() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	stmt, err := s.db.Prepare(fmt.Sprintf(`create table "%s" (
		"id" serial primary key,
		"created_at" timestamp not null default now(),
		"processed_at" timestamp null,
		"destination" text not null,
		"key" bytea not null,
		"value" bytea null
	)`, testTable))
	s.Require().NoError(err)
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx)
	s.Require().NoError(err)
}

func (s *OutboxTestSuite) migrateDown() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	stmt, err := s.db.Prepare(fmt.Sprintf(`drop table if exists "%s"`, testTable))
	s.Require().NoError(err)
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx)
	s.Require().NoError(err)
}

func (s *OutboxTestSuite) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	stmt, err := s.db.Prepare(fmt.Sprintf(`truncate "%s"`, testTable))
	s.Require().NoError(err)
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx)
	s.Require().NoError(err)
}

func (s *OutboxTestSuite) assertEmptyOutbox() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	stmt, err := s.db.Prepare(fmt.Sprintf(`select count(*) from "%s"`, testTable))
	s.Require().NoError(err)
	defer stmt.Close()

	var count int
	err = stmt.QueryRowContext(ctx).Scan(&count)
	s.Require().NoError(err)
	s.Zero(count)
}

func (s *OutboxTestSuite) assertWholeOutboxProcessed() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	stmt, err := s.db.Prepare(fmt.Sprintf(`
		select count(*) from "%s" where processed_at is null
	`, testTable))
	s.Require().NoError(err)
	defer stmt.Close()

	processedStmt, err := s.db.Prepare(fmt.Sprintf(`
		select count(*) from "%s" where processed_at is not null
	`, testTable))
	s.Require().NoError(err)
	defer processedStmt.Close()

	var count int
	err = stmt.QueryRowContext(ctx).Scan(&count)
	s.Require().NoError(err)
	s.Zero(count, "there should be no unprocessed messages")

	var processedCount int
	err = processedStmt.QueryRowContext(ctx).Scan(&processedCount)
	s.Require().NoError(err)
	s.True(processedCount > count, "there should be some processed messages")
}

func (s *OutboxTestSuite) assertOutboxMessages(expected ...Message) {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	stmt, err := s.db.Prepare(fmt.Sprintf(`
		select destination, key, value from "%s" where processed_at is null order by id asc
	`, testTable))
	s.Require().NoError(err)
	defer stmt.Close()

	var actual []Message
	rows, err := stmt.QueryContext(ctx)
	s.Require().NoError(err)
	defer rows.Close()

	for rows.Next() {
		var msg Message
		err = rows.Scan(&msg.Destination, &msg.Key, &msg.Value)
		s.Require().NoError(err)
		actual = append(actual, msg)
	}
	s.Require().NoError(rows.Err())

	s.ElementsMatch(expected, actual)
}

func mockMessages(num int) []Message {
	messages := make([]Message, 0, num)
	for i := 0; i < num; i++ {
		messages = append(messages, Message{
			Destination: "test-topic",
			Key:         []byte(fmt.Sprintf("test-key-%d", i)),
			Value:       []byte(fmt.Sprintf("test-value-%d", i)),
		})
	}
	return messages
}
