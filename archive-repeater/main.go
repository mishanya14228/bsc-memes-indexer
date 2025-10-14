package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/joho/godotenv"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/fourmeme"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/pancake"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/queue"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const batchSize = 20000

var errNoRecords = errors.New("no records available")

type Repeater struct {
	mongoClient *mongo.Client
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
}

type LogRecord struct {
	ID          primitive.ObjectID `bson:"_id"`
	Source      string             `bson:"source"`
	BlockNumber uint64             `bson:"blockNumber"`
	Timestamp   uint64             `bson:"blockTime"`
	TxHash      string             `bson:"txHash"`
	LogIndex    uint32             `bson:"logIndex"`
	Address     string             `bson:"address"`
	Topics      []string           `bson:"topics"`
	Data        string             `bson:"data"`
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[info] .env file not found, relying on environment variables")
	}

	mongoURL := os.Getenv("MONGODB_URI")
	rabbitmqURL := os.Getenv("RABBITMQ_URL")

	if mongoURL == "" || rabbitmqURL == "" {
		log.Fatalf("[fatal] MONGODB_URI and RABBITMQ_URL must be set")
	}

	repeater, err := NewRepeater(mongoURL, rabbitmqURL)
	if err != nil {
		log.Fatalf("[fatal] could not create repeater: %v", err)
	}
	defer repeater.Close()

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := repeater.processBatch(ctx)
				switch {
				case errors.Is(err, errNoRecords):
					log.Println("[info] no more records to process, sleeping for 30 seconds")
					time.Sleep(30 * time.Second)
				case err != nil:
					log.Printf("[error] failed to process batch: %v", err)
					time.Sleep(10 * time.Second)
				default:
					// Successful batch; avoid tight loop.
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	}()

	<-sigChan
	log.Println("[info] shutting down...")
	cancel()
	// allow processing loop to exit cleanly
	time.Sleep(1 * time.Second)
}

func NewRepeater(mongoURL, rabbitmqURL string) (*Repeater, error) {
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

	return &Repeater{
		mongoClient: mongoClient,
		amqpConn:    amqpConn,
		amqpChannel: amqpChannel,
	}, nil
}

func (r *Repeater) processBatch(ctx context.Context) error {
	collectionName := os.Getenv("MONGODB_COLLECTION")
	if collectionName == "" {
		collectionName = "historical_logs"
	}
	dbName := os.Getenv("MONGODB_DATABASE")
	if dbName == "" {
		dbName = "bsc-memes"
	}

	log.Printf("[debug] using database: %s, collection: %s", dbName, collectionName)

	collection := r.mongoClient.Database(dbName).Collection(collectionName)

	findOpts := options.Find().SetLimit(batchSize)

	cursor, err := collection.Find(ctx, bson.M{}, findOpts)
	if err != nil {
		return fmt.Errorf("failed to find documents: %w", err)
	}
	defer cursor.Close(ctx)

	var records []LogRecord
	if err := cursor.All(ctx, &records); err != nil {
		return fmt.Errorf("failed to decode documents: %w", err)
	}

	if len(records) == 0 {
		return errNoRecords
	}

	log.Printf("[info] processing %d records", len(records))

	processedIDs := make([]primitive.ObjectID, 0, len(records))

	for _, record := range records {
		data, err := hexutil.Decode(record.Data)
		if err != nil {
			log.Printf("[error] failed to decode log data: %v", err)
			continue
		}

		logEntry := types.Log{
			Address:     common.HexToAddress(record.Address),
			Topics:      make([]common.Hash, len(record.Topics)),
			Data:        data,
			BlockNumber: record.BlockNumber,
			TxHash:      common.HexToHash(record.TxHash),
			Index:       uint(record.LogIndex),
		}
		for i, t := range record.Topics {
			logEntry.Topics[i] = common.HexToHash(t)
		}

		switch record.Source {
		case "four-meme":
			trade, err := fourmeme.ParseTradeFromLog(logEntry, record.Timestamp)
			if err != nil {
				log.Printf("[error] failed to parse four-meme trade: %v", err)
				continue
			}
			body, err := json.Marshal(trade)
			if err != nil {
				log.Printf("[error] failed to marshal four-meme trade: %v", err)
				continue
			}
			if err := r.publish(ctx, queue.TradesQueue, body); err != nil {
				log.Printf("[error] failed to publish four-meme trade: %v", err)
				continue
			}
		case "pancake":
			swap, err := pancake.ParseSwapFromLog(logEntry, record.Timestamp)
			if err != nil {
				log.Printf("[error] failed to parse pancake swap: %v", err)
				continue
			}
			body, err := json.Marshal(swap)
			if err != nil {
				log.Printf("[error] failed to marshal pancake swap: %v", err)
				continue
			}
			if err := r.publish(ctx, queue.PoolSwapsQueue, body); err != nil {
				log.Printf("[error] failed to publish pancake swap: %v", err)
				continue
			}
		default:
			log.Printf("[warn] unknown log source: %s", record.Source)
			continue
		}

		processedIDs = append(processedIDs, record.ID)
	}

	if len(processedIDs) > 0 {
		r.deleteIDs(ctx, collection, processedIDs)
	}

	return nil
}

func (r *Repeater) publish(ctx context.Context, queueName string, body []byte) error {
	return r.amqpChannel.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func (r *Repeater) deleteIDs(ctx context.Context, collection *mongo.Collection, ids []primitive.ObjectID) {
	result, err := collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		log.Printf("[error] failed to delete documents: %v", err)
		return
	}
	if result.DeletedCount > 0 {
		log.Printf("[info] deleted %d processed documents", result.DeletedCount)
	}
}

func (r *Repeater) Close() {
	if r.amqpChannel != nil {
		r.amqpChannel.Close()
	}
	if r.amqpConn != nil {
		r.amqpConn.Close()
	}
	if r.mongoClient != nil {
		r.mongoClient.Disconnect(context.Background())
	}
}
