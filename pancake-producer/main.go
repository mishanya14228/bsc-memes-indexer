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
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/pancake"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/queue"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/redis_keys"
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

	// --- DLQ Setup for PoolSwapsQueue ---
	dlxName := "pool-swaps-dlx"

	// Declare the dead-letter exchange (robustly, in case relay hasn't)
	err = amqpChannel.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to declare dlx: %w", err)
	}

	// Arguments for the main queue to link it to the DLX
	args := amqp.Table{
		"x-dead-letter-exchange": dlxName,
	}

	// Declare the main queue with DLQ args
	_, err = amqpChannel.QueueDeclare(queue.PoolSwapsQueue, true, false, false, false, args)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %w", queue.PoolSwapsQueue, err)
	}

	// Declare the skipped blocks queue (without DLQ)
	_, err = amqpChannel.QueueDeclare(queue.PancakeSkippedBlocksQueue, true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %w", queue.PancakeSkippedBlocksQueue, err)
	}

	return &Producer{
		ethWSSClient:  ethWSSClient,
		ethHTTPClient: ethHTTPClient,
		redisClient:   redisClient,
		amqpConn:      amqpConn,
		amqpChannel:   amqpChannel,
	}, nil
}

// handleStartupCatchup checks for missed blocks and publishes the range to a queue.
func (p *Producer) handleStartupCatchup(ctx context.Context) error {
	lastBlockStr, err := p.redisClient.Get(ctx, redis_keys.LastBlockPancake).Result()
	if errors.Is(err, redis.Nil) {
		log.Println("[info] No last processed block found in Redis (first run). Starting from current block.")
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

		if err := p.amqpChannel.PublishWithContext(ctx, "", queue.PancakeSkippedBlocksQueue, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		}); err != nil {
			return fmt.Errorf("failed to publish skipped block range: %w", err)
		}
		log.Printf("[info] Published skipped block range %d-%d to queue %s", skippedRange.Start, skippedRange.End, queue.PancakeSkippedBlocksQueue)

		if err := p.redisClient.Del(ctx, redis_keys.LastBlockPancake).Err(); err != nil {
			return fmt.Errorf("failed to delete redis key: %w", err)
		}
		log.Printf("[info] Consumed and deleted checkpoint key from Redis: %s", redis_keys.LastBlockPancake)
	}

	return nil
}

// startSubscription starts the WebSocket subscription.
func (p *Producer) startSubscription(ctx context.Context) (<-chan *trade.Swap, <-chan error) {
	logs := make(chan types.Log)
	swaps := make(chan *trade.Swap)
	errChan := make(chan error, 1)

	query := ethereum.FilterQuery{
		Topics: [][]common.Hash{{pancake.SwapEventTopic()}},
	}

	sub, err := p.ethWSSClient.SubscribeFilterLogs(ctx, query, logs)
	if err != nil {
		errChan <- err
		return nil, errChan
	}

	go func() {
		defer close(swaps)
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
				processedSwap, err := p.processLog(vLog)
				if err != nil {
					log.Printf("[warn] failed to process log: %v", err)
					continue
				}
				swaps <- processedSwap
			}
		}
	}()

	return swaps, errChan
}

// Publish sends a swap to the RabbitMQ queue.
func (p *Producer) Publish(ctx context.Context, swap *trade.Swap) error {
	body, err := json.Marshal(swap)
	if err != nil {
		return fmt.Errorf("failed to marshal swap: %w", err)
	}
	return p.amqpChannel.PublishWithContext(ctx, "", queue.PoolSwapsQueue, false, false, amqp.Publishing{
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
		if err := p.redisClient.Set(ctx, redis_keys.LastBlockPancake, lastBlock, 0).Err(); err != nil {
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

func (p *Producer) processLog(vLog types.Log) (*trade.Swap, error) {
	return pancake.ParseSwapFromLog(vLog, uint64(time.Now().Unix()))
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
		producer.Close(context.Background())
		cancel()
	}()

	if err := producer.handleStartupCatchup(ctx); err != nil {
		log.Fatalf("[fatal] failed during startup catchup: %v", err)
	}

	swaps, errChan := producer.startSubscription(ctx)

	log.Println("[info] starting pancake swap producer...")

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
		case swap, ok := <-swaps:
			if !ok {
				log.Println("[info] swaps channel closed.")
				return
			}

			producer.lastProcessedBlock.Store(swap.Block)
			if err := producer.Publish(ctx, swap); err != nil {
				log.Printf("[warn] failed to publish swap: %v", err)
			} else {
				log.Printf("[info] published swap from pool %s in tx %s", swap.PoolAddress.Hex(), swap.TxHash.Hex())
			}
		}
	}
}
