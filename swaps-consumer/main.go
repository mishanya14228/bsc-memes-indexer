package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/trade"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	bufferSize    = 1500
	bufferTimeout = 30 * time.Second
)

type Consumer struct {
	db          *sql.DB
	mongoClient *mongo.Client
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
	messages    <-chan amqp.Delivery
	buffer      []amqp.Delivery
	mu          sync.Mutex
	ticker      *time.Ticker
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[info] .env file not found, relying on environment variables")
	}

	postgresURL := os.Getenv("POSTGRES_URL")
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	mongoURL := os.Getenv("MONGODB_URI")

	if postgresURL == "" || rabbitmqURL == "" || mongoURL == "" {
		log.Fatalf("[fatal] POSTGRES_URL, RABBITMQ_URL and MONGODB_URI must be set")
	}

	consumer, err := NewConsumer(postgresURL, rabbitmqURL, mongoURL)
	if err != nil {
		log.Fatalf("[fatal] could not create consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go consumer.run(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[info] shutting down...")
	cancel()
	// Wait for a moment to allow the run loop to finish processing buffered messages
	time.Sleep(2 * time.Second)
}

func NewConsumer(postgresURL, rabbitmqURL, mongoURL string) (*Consumer, error) {
	// Check if sslmode is already set
	if !strings.Contains(postgresURL, "sslmode") {
		postgresURL += "?sslmode=disable"
	}
	db, err := sql.Open("postgres", postgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongo: %w", err)
	}

	amqpConn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	amqpChannel, err := amqpConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	if err := amqpChannel.Qos(5000, 0, false); err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	_, err = amqpChannel.QueueDeclare(
		"trades", // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	msgs, err := amqpChannel.Consume(
		"trades", // queue
		"",       // consumer
		false,    // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %w", err)
	}

	return &Consumer{
		db:          db,
		mongoClient: mongoClient,
		amqpConn:    amqpConn,
		amqpChannel: amqpChannel,
		messages:    msgs,
		buffer:      make([]amqp.Delivery, 0, bufferSize),
		ticker:      time.NewTicker(bufferTimeout),
	}, nil
}

func (c *Consumer) run(ctx context.Context) {
	log.Println("[info] starting consumer...")
	for {
		select {
		case <-ctx.Done():
			log.Println("[info] context cancelled, processing remaining messages in buffer")
			c.mu.Lock()
			c.processBuffer()
			c.mu.Unlock()
			return
		case msg := <-c.messages:
			c.mu.Lock()
			c.buffer = append(c.buffer, msg)
			if len(c.buffer) >= bufferSize {
				c.processBuffer()
				c.ticker.Reset(bufferTimeout)
			}
			c.mu.Unlock()
		case <-c.ticker.C:
			c.mu.Lock()
			if len(c.buffer) > 0 {
				c.processBuffer()
			}
			c.mu.Unlock()
		}
	}
}

func (c *Consumer) processBuffer() {
	if len(c.buffer) == 0 {
		return
	}

	log.Printf("[info] processing buffer of size %d", len(c.buffer))

	if err := c.bulkInsert(c.buffer); err != nil {
		log.Printf("[error] bulk insert failed: %v.", err)
		// Nack all messages so they can be retried
		for _, msg := range c.buffer {
			msg.Nack(false, true)
		}
	} else {
		// Ack all messages
		for _, msg := range c.buffer {
			msg.Ack(false)
		}
	}

	c.buffer = make([]amqp.Delivery, 0, bufferSize)
}

func (c *Consumer) bulkInsert(messages []amqp.Delivery) error {
	if len(messages) == 0 {
		return nil
	}

	valueStrings := make([]string, 0, len(messages))
	valueArgs := make([]interface{}, 0, len(messages)*9)
	for i, msg := range messages {
		var t trade.Trade
		if err := json.Unmarshal(msg.Body, &t); err != nil {
			log.Printf("[error] failed to unmarshal trade: %v", err)
			msg.Nack(false, false)
			continue
		}

		scale := big.NewInt(1_000_000_000)
		tokensAmount := new(big.Int).Div(t.TokensAmount, scale).String()
		bnbAmount := new(big.Int).Div(t.BnbAmount, scale).String()

		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*9+1, i*9+2, i*9+3, i*9+4, i*9+5, i*9+6, i*9+7, i*9+8, i*9+9))
		valueArgs = append(valueArgs, time.Unix(int64(t.Timestamp), 0))
		valueArgs = append(valueArgs, t.Block)
		valueArgs = append(valueArgs, strings.ToLower(t.Trader.Hex()))
		valueArgs = append(valueArgs, strings.ToLower(t.Direction))
		valueArgs = append(valueArgs, strings.ToLower(t.Token.Hex()))
		valueArgs = append(valueArgs, tokensAmount)
		valueArgs = append(valueArgs, bnbAmount)
		valueArgs = append(valueArgs, strings.ToLower(t.TxHash.Hex()))
		valueArgs = append(valueArgs, t.Platform)
	}

	stmt := fmt.Sprintf(`INSERT INTO bsc_swaps (block_time, block, trader_addr, direction, token_addr, token_amount, bnb_amount, tx_hash, platform) VALUES %s ON CONFLICT (tx_hash, trader_addr) DO NOTHING`, strings.Join(valueStrings, ","))

	_, err := c.db.Exec(stmt, valueArgs...)
	return err
}

func (c *Consumer) insertSingle(t *trade.Trade) error {
	_, err := c.db.Exec(`
		INSERT INTO bsc_swaps (block_time, block, trader_addr, direction, token_addr, token_amount, bnb_amount, tx_hash, platform)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (tx_hash, trader_addr) DO NOTHING`,
		time.Unix(int64(t.Timestamp), 0),
		t.Block,
		t.Trader.Hex(),
		strings.ToLower(t.Direction),
		t.Token.Hex(),
		t.TokensAmount.Int64(),
		t.BnbAmount.Int64(),
		t.TxHash.Hex(),
		t.Platform,
	)
	return err
}

func (c *Consumer) saveFailedSwap(body []byte) {
	collectionName := os.Getenv("MONGODB_FAILED_SWAPS_COLLECTION")
	if collectionName == "" {
		collectionName = "failed_swaps"
	}
	dbName := os.Getenv("MONGODB_DATABASE")
	if dbName == "" {
		dbName = "bsc-memes"
	}

	collection := c.mongoClient.Database(dbName).Collection(collectionName)
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		// If we can't even unmarshal it, save the raw body
		data = map[string]interface{}{"raw_body": string(body)}
	}

	_, err := collection.InsertOne(context.Background(), data)
	if err != nil {
		log.Printf("[error] failed to save failed swap to mongo: %v", err)
	} else {
		log.Println("[info] successfully saved failed swap to mongo")
	}
}

func (c *Consumer) Close() {
	if c.amqpChannel != nil {
		c.amqpChannel.Close()
	}
	if c.amqpConn != nil {
		c.amqpConn.Close()
	}
	if c.db != nil {
		c.db.Close()
	}
	if c.mongoClient != nil {
		c.mongoClient.Disconnect(context.Background())
	}
}
