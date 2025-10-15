package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/queue"
	"log"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/contracts"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/pancake"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/topics"
	amqp "github.com/rabbitmq/amqp091-go"
	bson "go.mongodb.org/mongo-driver/bson"
	goMongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

const (
	chunkSize           uint64 = 10
	maxConcurrentChunks        = 10
	retryInitialDelay          = 1 * time.Second
	retryMaxDelay              = 1 * time.Minute
)

type logSource string

const (
	sourceFourMeme logSource = "four-meme"
	sourcePancake  logSource = "pancake"
)

type blockRange struct {
	start uint64
	end   uint64
}

type SkippedBlockRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

func runListener() error {
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		return fmt.Errorf("RABBITMQ_URL must be set")
	}

	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %w", err)
	}
	defer ch.Close()

	queues := []string{queue.FourMemeSkippedBlocksQueue, queue.PancakeSkippedBlocksQueue}
	for _, q := range queues {
		_, err = ch.QueueDeclare(q, true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("failed to declare queue %s: %w", q, err)
		}
	}

	forever := make(chan bool)

	for _, q := range queues {
		msgs, err := ch.Consume(
			q,    // queue
			"",   // consumer
			true, // auto-ack
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to register a consumer for queue %s: %w", q, err)
		}

		go func(queueName string) {
			for d := range msgs {
				log.Printf("Received a message from %s", queueName)
				var skippedRange SkippedBlockRange
				if err := json.Unmarshal(d.Body, &skippedRange); err != nil {
					log.Printf("[error] failed to unmarshal skipped block range: %v", err)
					continue
				}

				log.Printf("[info] processing skipped block range %d-%d", skippedRange.Start, skippedRange.End)
				if err := run(context.Background(), skippedRange.Start, skippedRange.End); err != nil {
					log.Printf("[error] failed to process skipped block range: %v", err)
				}
			}
		}(q)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

	return nil
}
func main() {
	fromBlock := flag.Uint64("from", 0, "starting block (inclusive)")
	toBlock := flag.Uint64("to", 0, "ending block (inclusive)")
	flag.Parse()

	if *fromBlock > 0 && *toBlock > 0 {
		// Manual mode
		if *fromBlock > *toBlock {
			log.Fatalf("[fatal] from block (%d) must be less than or equal to to block (%d)", *fromBlock, *toBlock)
		}
		if err := godotenv.Load(); err != nil {
			log.Println("[info] .env file not found, relying on environment variables")
		}
		if err := run(context.Background(), *fromBlock, *toBlock); err != nil {
			log.Fatalf("[fatal] historical fetch failed: %v", err)
		}
	} else {
		// Listener mode
		log.Println("[info] running in listener mode")
		if err := godotenv.Load(); err != nil {
			log.Println("[info] .env file not found, relying on environment variables")
		}
		if err := runListener(); err != nil {
			log.Fatalf("[fatal] listener failed: %v", err)
		}
	}
}

func run(ctx context.Context, fromBlock, toBlock uint64) error {
	rpcURL := os.Getenv("BSC_ARCHIVE_RPC_URL")
	if rpcURL == "" {
		rpcURL = os.Getenv("BSC_RPC_URL")
	}
	if rpcURL == "" {
		return fmt.Errorf("BSC_ARCHIVE_RPC_URL or BSC_RPC_URL must be set")
	}

	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		return fmt.Errorf("MONGODB_URI must be set")
	}

	databaseName := os.Getenv("MONGODB_DATABASE")
	if databaseName == "" {
		databaseName = "bsc-memes"
	}

	collectionName := os.Getenv("MONGODB_COLLECTION")
	if collectionName == "" {
		collectionName = "historical_logs"
	}

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return fmt.Errorf("failed to dial eth client: %w", err)
	}
	defer client.Close()

	mongoClient, collection, err := newMongoCollection(ctx, mongoURI, databaseName, collectionName)
	if err != nil {
		return err
	}
	defer func() {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mongoClient.Disconnect(disconnectCtx); err != nil {
			log.Printf("[warn] failed to disconnect mongo client: %v", err)
		}
	}()

	timestamps := newTimestampCache()
	ranges := buildRanges(fromBlock, toBlock)
	log.Printf("[info] processing %d chunk(s) covering range %d-%d with concurrency %d", len(ranges), fromBlock, toBlock, maxConcurrentChunks)

	totalBlocksToProcess := toBlock - fromBlock + 1
	var processedBlocks atomic.Uint64

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentChunks)

	for _, r := range ranges {
		r := r
		g.Go(func() error {
			if err := processChunk(gCtx, client, collection, timestamps, r); err != nil {
				return err
			}

			chunkBlockCount := r.end - r.start + 1
			processedCount := processedBlocks.Add(chunkBlockCount)
			remainingCount := totalBlocksToProcess - processedCount

			log.Printf("[info] Finished chunk %d-%d. Blocks remaining: %d", r.start, r.end, remainingCount)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	log.Printf("[info] completed processing for range %d-%d", fromBlock, toBlock)
	return nil
}

func buildRanges(fromBlock, toBlock uint64) []blockRange {
	ranges := make([]blockRange, 0)
	for start := fromBlock; start <= toBlock; {
		end := start + chunkSize - 1
		if end > toBlock {
			end = toBlock
		}
		ranges = append(ranges, blockRange{start: start, end: end})
		if end == toBlock {
			break
		}
		start = end + 1
	}
	return ranges
}

func newMongoCollection(ctx context.Context, uri, database, collection string) (*goMongo.Client, *goMongo.Collection, error) {
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := goMongo.Connect(connectCtx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to mongo: %w", err)
	}

	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPing()
	if err := client.Ping(pingCtx, nil); err != nil {
		return nil, nil, fmt.Errorf("failed to ping mongo: %w", err)
	}

	return client, client.Database(database).Collection(collection), nil
}

func processFourMemeRange(ctx context.Context, client *ethclient.Client, coll *goMongo.Collection, cache *timestampCache, start, end uint64) error {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(start)),
		ToBlock:   big.NewInt(int64(end)),
		Addresses: []common.Address{common.HexToAddress(contracts.FourMemeContractAddress)},
		Topics:    [][]common.Hash{{topics.FourMemeBuyTopic, topics.FourMemeSellTopic}},
	}

	return processRange(ctx, client, coll, cache, start, end, query, sourceFourMeme)
}

func processPancakeRange(ctx context.Context, client *ethclient.Client, coll *goMongo.Collection, cache *timestampCache, start, end uint64) error {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(start)),
		ToBlock:   big.NewInt(int64(end)),
		Topics:    [][]common.Hash{{pancake.SwapEventTopic()}},
	}

	return processRange(ctx, client, coll, cache, start, end, query, sourcePancake)
}

func processChunk(ctx context.Context, client *ethclient.Client, coll *goMongo.Collection, cache *timestampCache, r blockRange) error {
	if err := processFourMemeRange(ctx, client, coll, cache, r.start, r.end); err != nil {
		return fmt.Errorf("four.meme range %d-%d: %w", r.start, r.end, err)
	}

	if err := processPancakeRange(ctx, client, coll, cache, r.start, r.end); err != nil {
		return fmt.Errorf("pancake range %d-%d: %w", r.start, r.end, err)
	}

	return nil
}

func processRange(ctx context.Context, client *ethclient.Client, coll *goMongo.Collection, cache *timestampCache, start, end uint64, query ethereum.FilterQuery, source logSource) error {
	logs, err := fetchLogsWithRetry(ctx, client, query)
	if err != nil {
		return err
	}

	if len(logs) == 0 {
		log.Printf("[info] no %s logs found for %d-%d", source, start, end)
		return nil
	}

	docs := make([]interface{}, 0, len(logs))
	for _, l := range logs {
		ts, err := cache.Get(ctx, client, l.BlockNumber)
		if err != nil {
			return fmt.Errorf("failed to fetch timestamp for block %d: %w", l.BlockNumber, err)
		}

		docs = append(docs, buildLogDocument(source, start, end, ts, l))
	}

	if err := insertDocumentsWithRetry(ctx, coll, docs); err != nil {
		return err
	}

	log.Printf("[info] inserted %d %s logs for %d-%d", len(docs), source, start, end)
	return nil
}

func buildLogDocument(source logSource, chunkStart, chunkEnd, blockTimestamp uint64, l types.Log) bson.M {
	topics := make([]string, len(l.Topics))
	for i, topic := range l.Topics {
		topics[i] = topic.Hex()
	}

	blockHash := ""
	if l.BlockHash != (common.Hash{}) {
		blockHash = l.BlockHash.Hex()
	}

	return bson.M{
		"source":      string(source),
		"chunkStart":  chunkStart,
		"chunkEnd":    chunkEnd,
		"blockNumber": l.BlockNumber,
		"blockHash":   blockHash,
		"blockTime":   blockTimestamp,
		"txHash":      l.TxHash.Hex(),
		"txIndex":     int32(l.TxIndex),
		"logIndex":    int32(l.Index),
		"address":     l.Address.Hex(),
		"topics":      topics,
		"data":        hexutil.Encode(l.Data),
		"removed":     l.Removed,
		"retrievedAt": time.Now().UTC(),
	}
}

func fetchLogsWithRetry(ctx context.Context, client *ethclient.Client, query ethereum.FilterQuery) ([]types.Log, error) {
	attempt := 1
	delay := retryInitialDelay

	for {
		logs, err := client.FilterLogs(ctx, query)
		if err == nil {
			return logs, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		log.Printf("[warn] failed to fetch logs (attempt %d): %v. Retrying in %s...", attempt, err, delay)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		delay = nextDelay(delay)
		attempt++
	}
}

func insertDocumentsWithRetry(ctx context.Context, coll *goMongo.Collection, docs []interface{}) error {
	attempt := 1
	delay := retryInitialDelay

	for {
		_, err := coll.InsertMany(ctx, docs)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("[warn] failed to insert %d documents (attempt %d): %v. Retrying in %s...", len(docs), attempt, err, delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		delay = nextDelay(delay)
		attempt++
	}
}

func nextDelay(current time.Duration) time.Duration {
	next := current * 2
	if next > retryMaxDelay {
		return retryMaxDelay
	}
	return next
}

type timestampCache struct {
	mu     sync.Mutex
	values map[uint64]uint64
}

func newTimestampCache() *timestampCache {
	return &timestampCache{values: make(map[uint64]uint64)}
}

func (c *timestampCache) Get(ctx context.Context, client *ethclient.Client, block uint64) (uint64, error) {
	c.mu.Lock()
	if ts, ok := c.values[block]; ok {
		c.mu.Unlock()
		return ts, nil
	}
	c.mu.Unlock()

	attempt := 1
	delay := retryInitialDelay

	for {
		header, err := client.HeaderByNumber(ctx, big.NewInt(int64(block)))
		if err == nil {
			ts := header.Time
			c.mu.Lock()
			c.values[block] = ts
			c.mu.Unlock()
			return ts, nil
		}

		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		log.Printf("[warn] failed to fetch header for block %d (attempt %d): %v. Retrying in %s...", block, attempt, err, delay)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(delay):
		}

		delay = nextDelay(delay)
		attempt++
	}
}
