package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/contracts"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/fourmeme"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/queue"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/redis_keys"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/topics"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/trade"
	amqp "github.com/rabbitmq/amqp091-go"
)

// SkippedBlockRange defines the message for the skipped blocks queue
type SkippedBlockRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

// Producer handles the full lifecycle of the service.
type Producer struct {
	ethWSSClient  *ethclient.Client
	ethHTTPClient *ethclient.Client
	redisClient   *redis.Client
	contract      common.Address
	amqpConn      *amqp.Connection
	amqpChannel   *amqp.Channel

	lastProcessedBlock atomic.Uint64
}

// NewProducer creates and returns a new Producer.
func NewProducer(wssURL, httpURL, rabbitmqURL, redisURL string) (*Producer, error) {
	ethWSSClient, err := ethclient.Dial(wssURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial eth wss client: %w", err)
	}

	ethHTTPClient, err := ethclient.Dial(httpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial eth http client: %w", err)
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis url: %w", err)
	}
	redisClient := redis.NewClient(opt)
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	amqpConn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial amqp: %w", err)
	}

	amqpChannel, err := amqpConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open amqp channel: %w", err)
	}

	queues := []string{queue.TradesQueue, queue.FourMemeSkippedBlocksQueue}
	for _, q := range queues {
		_, err = amqpChannel.QueueDeclare(q, true, false, false, false, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to declare queue %s: %w", q, err)
		}
	}

	return &Producer{
		ethWSSClient:  ethWSSClient,
		ethHTTPClient: ethHTTPClient,
		redisClient:   redisClient,
		contract:      common.HexToAddress(contracts.FourMemeContractAddress),
		amqpConn:      amqpConn,
		amqpChannel:   amqpChannel,
	}, nil
}

// handleStartupCatchup checks for missed blocks and publishes the range to a queue.
func (p *Producer) handleStartupCatchup(ctx context.Context) error {
	lastBlockStr, err := p.redisClient.Get(ctx, redis_keys.LastBlockFourMeme).Result()
	if errors.Is(err, redis.Nil) {
		log.Println("[info] No last processed block found in Redis (first run). Starting from current block.")
		// On first run, we set our last processed block to the current block to avoid a full history scan.
		currentBlock, err := p.ethHTTPClient.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("failed to get current block number on first run: %w", err)
		}
		p.lastProcessedBlock.Store(currentBlock)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get last block from redis: %w", err)
	}

	lastBlock, _ := strconv.ParseUint(lastBlockStr, 10, 64)
	p.lastProcessedBlock.Store(lastBlock)

	currentBlock, err := p.ethHTTPClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %w", err)
	}

	if lastBlock < currentBlock {
		log.Printf("[info] Gap detected. Last processed block: %d, Current block: %d", lastBlock, currentBlock)
		skippedRange := SkippedBlockRange{Start: lastBlock + 1, End: currentBlock}
		body, err := json.Marshal(skippedRange)
		if err != nil {
			return fmt.Errorf("failed to marshal skipped block range: %w", err)
		}

		if err := p.amqpChannel.PublishWithContext(ctx, "", queue.FourMemeSkippedBlocksQueue, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		}); err != nil {
			return fmt.Errorf("failed to publish skipped block range: %w", err)
		}
		log.Printf("[info] Published skipped block range %d-%d to queue %s", skippedRange.Start, skippedRange.End, queue.FourMemeSkippedBlocksQueue)

		// As requested, remove the key after it has been used.
		if err := p.redisClient.Del(ctx, redis_keys.LastBlockFourMeme).Err(); err != nil {
			return fmt.Errorf("failed to delete redis key: %w", err)
		}
		log.Printf("[info] Consumed and deleted checkpoint key from Redis: %s", redis_keys.LastBlockFourMeme)
	}

	return nil
}

// startSubscription starts the WebSocket subscription.
func (p *Producer) startSubscription(ctx context.Context) (<-chan *trade.Trade, <-chan error) {
	logs := make(chan types.Log)
	trades := make(chan *trade.Trade)
	errChan := make(chan error, 1)

	query := ethereum.FilterQuery{
		Addresses: []common.Address{p.contract},
		Topics:    [][]common.Hash{{topics.FourMemeBuyTopic, topics.FourMemeSellTopic}},
	}

	sub, err := p.ethWSSClient.SubscribeFilterLogs(ctx, query, logs)
	if err != nil {
		errChan <- err
		return nil, errChan
	}

	go func() {
		defer close(trades)
		defer close(errChan)

		for {
			select {
			case <-ctx.Done():
				log.Println("[info] context cancelled, stopping subscription...")
				sub.Unsubscribe()
				return
			case err := <-sub.Err():
				errChan <- err
				return
			case vLog := <-logs:
				processedTrade, err := p.processLog(vLog)
				if err != nil {
					log.Printf("[warn] failed to process log: %v", err)
					continue
				}
				trades <- processedTrade
			}
		}
	}()

	return trades, errChan
}

// Publish sends a trade to the RabbitMQ queue.
func (p *Producer) Publish(ctx context.Context, trade *trade.Trade) error {
	body, err := json.Marshal(trade)
	if err != nil {
		return fmt.Errorf("failed to marshal trade: %w", err)
	}
	return p.amqpChannel.PublishWithContext(ctx, "", queue.TradesQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

// Close handles graceful shutdown.
func (p *Producer) Close(ctx context.Context) {
	log.Println("[info] Saving last processed block to Redis...")
	lastBlock := p.lastProcessedBlock.Load()

	if lastBlock > 0 {
		if err := p.redisClient.Set(ctx, redis_keys.LastBlockFourMeme, lastBlock, 0).Err(); err != nil {
			log.Printf("[error] failed to save last processed block to redis: %v", err)
		} else {
			log.Printf("[info] Successfully saved last processed block: %d", lastBlock)
		}
	}

	p.ethWSSClient.Close()
	p.ethHTTPClient.Close()
	p.amqpChannel.Close()
	p.amqpConn.Close()
	p.redisClient.Close()
}

func (p *Producer) processLog(vLog types.Log) (*trade.Trade, error) {
	return fourmeme.ParseTradeFromLog(vLog, uint64(time.Now().Unix()))
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[info] .env file not found, relying on environment variables")
	}

	wssURL := os.Getenv("BSC_WSS_URL")
	httpURL := os.Getenv("BSC_RPC_URL")
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	redisURL := os.Getenv("REDIS_URL")
	if wssURL == "" || httpURL == "" || rabbitmqURL == "" || redisURL == "" {
		log.Fatalf("[fatal] BSC_WSS_URL, BSC_RPC_URL, RABBITMQ_URL, and REDIS_URL must be set")
	}

	ctx, cancel := context.WithCancel(context.Background())

	producer, err := NewProducer(wssURL, httpURL, rabbitmqURL, redisURL)
	if err != nil {
		log.Fatalf("[fatal] failed to create producer: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("[info] received signal, shutting down...")
		producer.Close(context.Background()) // Use a background context for cleanup
		cancel()
	}()

	// Handle startup catchup logic
	if err := producer.handleStartupCatchup(ctx); err != nil {
		log.Fatalf("[fatal] failed during startup catchup: %v", err)
	}

	// Start real-time processing
	trades, errChan := producer.startSubscription(ctx)

	log.Println("[info] starting real-time event listener...")

	for {
		select {
		case <-ctx.Done():
			log.Println("[info] producer shut down cleanly.")
			return
		case err, ok := <-errChan:
			if !ok {
				log.Println("[info] subscription error channel closed.")
				return
			}
			if err != nil {
				log.Printf("[error] subscription failed: %v", err)
			}
		case trade, ok := <-trades:
			if !ok {
				log.Println("[info] trades channel closed.")
				return
			}

			// Update last processed block and publish
			producer.lastProcessedBlock.Store(trade.Block)
			if err := producer.Publish(ctx, trade); err != nil {
				log.Printf("[warn] failed to publish trade: %v", err)
			} else {
				log.Printf("[info] published trade %s", trade.TxHash.Hex())
			}
		}
	}
}
