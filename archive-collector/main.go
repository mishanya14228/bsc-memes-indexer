package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/queue"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

const defaultArchiveDB = "archives"

type Collector struct {
	amqpConn    *amqp.Connection
	mongoClient *mongo.Client
	database    *mongo.Database
}

func NewCollector(rabbitURL, mongoURI, dbName string) (*Collector, error) {
	if dbName == "" {
		dbName = defaultArchiveDB
	}

	amqpConn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, err
	}

	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		amqpConn.Close()
		return nil, err
	}

	return &Collector{
		amqpConn:    amqpConn,
		mongoClient: mongoClient,
		database:    mongoClient.Database(dbName),
	}, nil
}

func (c *Collector) Close() {
	if c.amqpConn != nil {
		c.amqpConn.Close()
	}
	if c.mongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.mongoClient.Disconnect(ctx)
	}
}

func (c *Collector) Run(ctx context.Context) error {
	queues := []string{queue.ArchiveBlocksQueue, queue.ArchiveLogsQueue}
	g, ctx := errgroup.WithContext(ctx)

	for _, q := range queues {
		queueName := q
		g.Go(func() error {
			channel, err := c.amqpConn.Channel()
			if err != nil {
				return err
			}
			defer channel.Close()

			if _, err := channel.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
				return err
			}

			if err := channel.Qos(100, 0, false); err != nil {
				return err
			}

			messages, err := channel.Consume(queueName, "", false, false, false, false, nil)
			if err != nil {
				return err
			}

			collection := c.database.Collection(queueName)

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case msg, ok := <-messages:
					if !ok {
						return nil
					}

					if err := c.storeMessage(ctx, collection, msg.Body); err != nil {
						log.Printf("[warn] failed to store message from %s: %v", queueName, err)
						_ = msg.Nack(false, true)
						continue
					}

					if err := msg.Ack(false); err != nil {
						log.Printf("[warn] failed to ack message from %s: %v", queueName, err)
					}
				}
			}
		})
	}

	return g.Wait()
}

func (c *Collector) storeMessage(ctx context.Context, collection *mongo.Collection, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	doc := bson.M{
		"receivedAt": time.Now().UTC(),
		"payload":    string(body),
	}

	_, err := collection.InsertOne(ctx, doc)
	return err
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[info] .env file not found, relying on environment variables")
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	mongoURI := os.Getenv("MONGODB_URI")
	archiveDB := os.Getenv("ARCHIVE_DB_NAME")

	if rabbitURL == "" || mongoURI == "" {
		log.Fatalf("[fatal] RABBITMQ_URL and MONGODB_URI must be set")
	}

	collector, err := NewCollector(rabbitURL, mongoURI, archiveDB)
	if err != nil {
		log.Fatalf("[fatal] failed to create collector: %v", err)
	}
	defer collector.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := collector.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[error] collector exited with error: %v", err)
			cancel()
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[info] shutdown signal received")
	cancel()
	wg.Wait()
	log.Println("[info] archive collector shut down cleanly")
}
