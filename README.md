# outbox

Implementation of the transactional outbox pattern in Go.

## Usage

Outbox package assumes you manage your database schema and migrations separately and that an outbox table with correct schema exists.

Create an outbox table like this:

```sql
create table mydb.outbox (
	"id" serial primary key,
	"created_at" timestamp not null default now(),
	"processed_at" timestamp null,
	"destination" text not null,
	"key" bytea not null,
	"value" bytea null
);
```

You also need to implement the `Producer` interface:

```go
type Producer interface {
	Send(ctx context.Context, topic string, key, val []byte) error
}
```

This could be your Kafka sarama producer or your email sending package etc. Create a new outbox instance:

```go
outbox := outbox.New(tableName, db, producer)
```

And then you can send messages to the outbox. Make sure to provide a transaction instance as sending of messages to the outbox must be wrapped by a transaction in your code to ensure atomicity.

```go
outbox.Send(ctx, tx, Message{
	Destination: "test-topic",
	Key:         []byte("test-key"),
	Value:       []byte("test-value"),
})
```

Flushing of outbox should be done outside of your transaction (after comitting successfully). I suggest flushing in a goroutine with separate context to avoid blocking the main application flow.

```go
func BusinessLogic(ctx context.Context) error {
    // Begin transaction
    //
    // Your business logic here.
    // Send a message to the outbox.
    //
    // Commit transaction

    go func(){
        outbox.Flush(context.Context())
    }()

    return nil
}
```

In order to cleanup old processed messages and also run flushing in the backround to pickup any failed flushes, use the outbox daemon:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

outboxDaemon := outbox.NewDaemon(outbox)
closeDaemon := outboxDaemon.Start(ctx)

defer func() {
    closeDaemon()
}()
```

## Development

Run integration tests (requires docker):

```sh
go test -tags it .
```

For generating mocks, I use [mockery](https://github.com/vektra/mockery).

```sh
go generate -tags it .
```