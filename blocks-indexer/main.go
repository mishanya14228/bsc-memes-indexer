package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/contracts"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/fourmeme"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/pancake"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/queue"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/redis_keys"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/topics"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/trade"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

const (
	retryDelayOnFailure = 3 * time.Second
)

// Indexer coordinates block streaming, log filtering, and publishing.
type Indexer struct {
	ethWSSClient  *ethclient.Client
	ethHTTPClient *ethclient.Client
	redisClient   *redis.Client
	amqpConn      *amqp.Connection
	amqpChannel   *amqp.Channel
	chainID       *big.Int

	lastProcessedBlock    atomic.Uint64
	blockBatchSize        uint64
	historicalConcurrency int
	archiveEnabled        bool
}

// NewIndexer wires dependencies and declares required queues.
func NewIndexer(wssURL, httpURL, rabbitmqURL, redisURL string, archiveEnabled bool) (*Indexer, error) {
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

	// Declare queues we publish to.
	if _, err := amqpChannel.QueueDeclare(queue.TradesQueue, true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %w", queue.TradesQueue, err)
	}
	if archiveEnabled {
		archiveQueues := []string{queue.ArchiveBlocksQueue, queue.ArchiveLogsQueue}
		for _, q := range archiveQueues {
			if _, err := amqpChannel.QueueDeclare(q, true, false, false, false, nil); err != nil {
				return nil, fmt.Errorf("failed to declare queue %s: %w", q, err)
			}
		}
	}

	dlxName := "pool-swaps-dlx"
	if err := amqpChannel.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("failed to declare dlx: %w", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    dlxName,
		"x-dead-letter-routing-key": queue.PoolSwapsQueue,
	}
	if _, err := amqpChannel.QueueDeclare(queue.PoolSwapsQueue, true, false, false, false, args); err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %w", queue.PoolSwapsQueue, err)
	}

	chainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	chainID, err := ethHTTPClient.NetworkID(chainCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch network id: %w", err)
	}
	log.Printf("[info] detected network chain ID %s", chainID.String())

	batchSizeStr := os.Getenv("BLOCKS_INDEXER_BATCH_SIZE")
	batchSize, err := strconv.ParseUint(batchSizeStr, 10, 64)
	if err != nil || batchSize == 0 {
		log.Printf("[info] BLOCKS_INDEXER_BATCH_SIZE not set or invalid, using default value of 10")
		batchSize = 10 // Default value
	}

	histConcurrency := 4
	if concStr := os.Getenv("BLOCKS_INDEXER_HISTORICAL_CONCURRENCY"); concStr != "" {
		if parsed, err := strconv.Atoi(concStr); err == nil && parsed > 0 {
			histConcurrency = parsed
		} else {
			log.Printf("[info] BLOCKS_INDEXER_HISTORICAL_CONCURRENCY not set or invalid, using default value of %d", histConcurrency)
		}
	}

	return &Indexer{
		ethWSSClient:          ethWSSClient,
		ethHTTPClient:         ethHTTPClient,
		redisClient:           redisClient,
		amqpConn:              amqpConn,
		amqpChannel:           amqpChannel,
		chainID:               chainID,
		blockBatchSize:        batchSize,
		historicalConcurrency: histConcurrency,
		archiveEnabled:        archiveEnabled,
	}, nil
}

// initializeCheckpoint loads the last processed block from Redis or seeds it with the current block number.
// An override greater than zero is treated as the last processed block value, bypassing Redis.
func (i *Indexer) initializeCheckpoint(ctx context.Context, override uint64) error {
	if override > 0 {
		i.lastProcessedBlock.Store(override)
		log.Printf("[info] startFrom override detected; treating block %d as already processed", override)
		return nil
	}

	lastBlockStr, err := i.redisClient.Get(ctx, redis_keys.LastBlockBlocksIndexer).Result()
	if errors.Is(err, redis.Nil) {
		currentBlock, err := i.ethHTTPClient.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("failed to get current block number on first run: %w", err)
		}
		i.lastProcessedBlock.Store(currentBlock)
		log.Printf("[info] No checkpoint found. Seeding last processed block with current block %d", currentBlock)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get last block from redis: %w", err)
	}

	lastBlock, err := parseUint(lastBlockStr)
	if err != nil {
		return fmt.Errorf("failed to parse last block %q from redis: %w", lastBlockStr, err)
	}
	i.lastProcessedBlock.Store(lastBlock)
	log.Printf("[info] Restored last processed block %d from Redis", lastBlock)
	return nil
}

// parseUint is a helper for parsing uint64 values.
func parseUint(value string) (uint64, error) {
	return strconv.ParseUint(value, 10, 64)
}

// persistCheckpoint saves the latest processed block number to Redis.
func (i *Indexer) persistCheckpoint(ctx context.Context) {
	last := i.lastProcessedBlock.Load()
	if last == 0 {
		return
	}
	if err := i.redisClient.Set(ctx, redis_keys.LastBlockBlocksIndexer, last, 0).Err(); err != nil {
		log.Printf("[warn] failed to persist checkpoint %d: %v", last, err)
	}
}

func (i *Indexer) updateLastProcessed(value uint64) bool {
	for {
		current := i.lastProcessedBlock.Load()
		if value <= current {
			return false
		}
		if i.lastProcessedBlock.CompareAndSwap(current, value) {
			return true
		}
	}
}

// Run performs a historical catch-up (if needed) and then starts the live subscription loop.
func (i *Indexer) Run(ctx context.Context, overrideStart uint64) error {
	if err := i.initializeCheckpoint(ctx, overrideStart); err != nil {
		return err
	}

	if err := i.catchUpHistorical(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	return i.runLive(ctx)
}

// catchUpHistorical replays any missed blocks between the stored checkpoint and the current chain tip.
func (i *Indexer) catchUpHistorical(ctx context.Context) error {
	workers := i.historicalConcurrency
	if workers < 1 {
		workers = 1
	}

	initialProcessed := i.lastProcessedBlock.Load()
	startTime := time.Now()
	progress := func(latest uint64) {
		current := i.lastProcessedBlock.Load()
		if current < initialProcessed {
			return
		}

		total := latest
		if total < initialProcessed {
			total = initialProcessed
		}
		if total <= initialProcessed {
			return
		}

		totalBlocks := total - initialProcessed
		processed := current - initialProcessed
		if processed > totalBlocks {
			totalBlocks = processed
		}
		remaining := totalBlocks - processed

		var percent float64
		if totalBlocks > 0 {
			percent = (float64(processed) / float64(totalBlocks)) * 100
		}

		etaStr := "estimating..."
		if remaining == 0 {
			etaStr = "0s"
		} else if processed > 0 {
			elapsed := time.Since(startTime)
			secondsPerBlock := elapsed.Seconds() / float64(processed)
			if secondsPerBlock < 0 {
				secondsPerBlock = 0
			}
			etaSeconds := secondsPerBlock * float64(remaining)
			if etaSeconds < 0 {
				etaSeconds = 0
			}
			eta := time.Duration(etaSeconds * float64(time.Second))
			etaStr = eta.Round(time.Second).String()
		}

		log.Printf("[info] historical catch-up progress: %d blocks left (%.2f%% complete), ETA %s", remaining, percent, etaStr)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		latest, err := i.ethHTTPClient.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("failed to get latest block number: %w", err)
		}

		next := i.lastProcessedBlock.Load() + 1
		if next > latest {
			return nil
		}

		batches := make([][]*types.Header, 0, workers)
		var firstRangeStart uint64
		for len(batches) < workers && next <= latest {
			start := next
			end := start + i.blockBatchSize - 1
			if end > latest {
				end = latest
			}

			headers := make([]*types.Header, 0, int(end-start+1))
			for number := start; number <= end; number++ {
				headers = append(headers, headerWithNumber(number))
			}

			if len(batches) == 0 {
				firstRangeStart = start
			}
			batches = append(batches, headers)
			next = end + 1
		}

		lastBatch := batches[len(batches)-1]
		lastRangeEnd := lastBatch[len(lastBatch)-1].Number.Uint64()
		log.Printf("[info] historical catch-up processing blocks %d-%d with concurrency %d", firstRangeStart, lastRangeEnd, len(batches))

		var eg errgroup.Group
		for _, batch := range batches {
			batch := batch
			eg.Go(func() error {
				return i.processBatch(ctx, batch)
			})
		}

		if err := eg.Wait(); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.Printf("[error] failed to process historical batches %d-%d: %v", firstRangeStart, lastRangeEnd, err)
			time.Sleep(retryDelayOnFailure)
			continue
		}

		progress(latest)
	}
}

// runLive subscribes to new block headers and processes them in batches.
func (i *Indexer) runLive(ctx context.Context) error {
	headers := make(chan *types.Header)
	sub, err := i.ethWSSClient.SubscribeNewHead(ctx, headers)
	if err != nil {
		return fmt.Errorf("failed to subscribe to new heads: %w", err)
	}
	defer sub.Unsubscribe()

	buffer := make([]*types.Header, 0, int(i.blockBatchSize))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			if err != nil {
				return fmt.Errorf("subscription error: %w", err)
			}
			return nil
		case header := <-headers:
			if header == nil {
				continue
			}

			last := i.lastProcessedBlock.Load()
			if header.Number == nil {
				continue
			}

			headerNumber := header.Number.Uint64()
			if headerNumber > 0 && last < headerNumber-1 {
				buffer = append(buffer, headerWithNumber(headerNumber-1))
			}

			if headerNumber <= last {
				continue
			}

			buffer = append(buffer, header)
			for len(buffer) >= int(i.blockBatchSize) {
				// Sort buffer to ensure proper ordering before creating batch
				sort.Slice(buffer, func(i, j int) bool {
					if buffer[i].Number == nil || buffer[j].Number == nil {
						return false
					}
					return buffer[i].Number.Cmp(buffer[j].Number) < 0
				})

				batch := make([]*types.Header, int(i.blockBatchSize))
				copy(batch, buffer[:int(i.blockBatchSize)])

				if err := i.processBatch(ctx, batch); err != nil {
					if errors.Is(err, context.Canceled) {
						return err
					}
					log.Printf("[error] failed to process live block batch %d-%d: %v", batch[0].Number.Uint64(), batch[len(batch)-1].Number.Uint64(), err)
					time.Sleep(retryDelayOnFailure)
					break
				}

				buffer = buffer[int(i.blockBatchSize):]
			}
		}
	}
}

// processBatch fetches logs and blocks for the provided headers, publishing any matching events.
func (i *Indexer) processBatch(ctx context.Context, headers []*types.Header) error {
	if len(headers) == 0 {
		return nil
	}

	// Sort headers by block number to ensure proper from/to range
	sort.Slice(headers, func(i, j int) bool {
		if headers[i].Number == nil || headers[j].Number == nil {
			return false
		}
		return headers[i].Number.Cmp(headers[j].Number) < 0
	})

	from := headers[0].Number
	to := headers[len(headers)-1].Number

	// Additional safety check to prevent invalid ranges
	if from != nil && to != nil && from.Cmp(to) > 0 {
		return fmt.Errorf("invalid block range: from (%s) > to (%s), headers count: %d", from.String(), to.String(), len(headers))
	}

	query := ethereum.FilterQuery{
		FromBlock: from,
		ToBlock:   to,
		Topics: [][]common.Hash{{
			pancake.SwapEventTopic(),
			topics.FourMemeBuyTopic,
			topics.FourMemeSellTopic,
		}},
	}

	logs, err := i.ethHTTPClient.FilterLogs(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to filter logs for range %s-%s: %w", from.String(), to.String(), err)
	}
	if i.archiveEnabled {
		fromNumber := uint64(0)
		toNumber := uint64(0)
		if from != nil {
			fromNumber = from.Uint64()
		}
		if to != nil {
			toNumber = to.Uint64()
		}
		if err := i.publishArchiveLogs(ctx, fromNumber, toNumber, logs); err != nil {
			log.Printf("[warn] failed to publish archive logs for range %d-%d: %v", fromNumber, toNumber, err)
		}
	}

	blocks := make(map[uint64]*types.Block, len(headers))
	txByHash := make(map[common.Hash]*types.Transaction)
	var fetchErrors []error

	for _, header := range headers {
		block, err := i.ethHTTPClient.BlockByNumber(ctx, header.Number)
		if err != nil {
			log.Printf("[warn] failed to fetch block %s: %v, will skip logs for this block", header.Number.String(), err)
			fetchErrors = append(fetchErrors, err)
			continue // Don't fail the entire batch, just skip this block
		}
		blocks[block.NumberU64()] = block
		if i.archiveEnabled {
			if err := i.publishArchiveBlock(ctx, block); err != nil {
				log.Printf("[warn] failed to publish archive block %d: %v", block.NumberU64(), err)
			}
		}
		for _, tx := range block.Transactions() {
			txByHash[tx.Hash()] = tx
		}
	}

	// If we failed to fetch any blocks, log it but continue processing available blocks
	if len(fetchErrors) > 0 {
		log.Printf("[warn] failed to fetch %d out of %d blocks in batch, processing available blocks only", len(fetchErrors), len(headers))
	}

	fourMemeAddress := common.HexToAddress(contracts.FourMemeContractAddress)
	swapTopic := pancake.SwapEventTopic()

	skippedLogs := 0
	for _, vLog := range logs {
		block := blocks[vLog.BlockNumber]
		if block == nil {
			log.Printf("[warn] missing block data for number %d (log tx: %s), skipping log processing", vLog.BlockNumber, vLog.TxHash.Hex())
			skippedLogs++
			continue
		}
		timestamp := block.Time()

		if vLog.Address == fourMemeAddress && len(vLog.Topics) > 0 && (vLog.Topics[0] == topics.FourMemeBuyTopic || vLog.Topics[0] == topics.FourMemeSellTopic) {
			tradeMsg, err := fourmeme.ParseTradeFromLog(vLog, timestamp)
			if err != nil {
				log.Printf("[warn] failed to parse fourmeme trade for tx %s: %v", vLog.TxHash.Hex(), err)
				continue
			}
			if err := i.publishTrade(ctx, tradeMsg); err != nil {
				log.Printf("[warn] failed to publish fourmeme trade %s: %v", tradeMsg.TxHash.Hex(), err)
			}
			continue
		}

		if len(vLog.Topics) > 0 && vLog.Topics[0] == swapTopic {
			swapMsg, err := pancake.ParseSwapFromLog(vLog, timestamp)
			if err != nil {
				log.Printf("[warn] failed to parse pancake swap for tx %s: %v", vLog.TxHash.Hex(), err)
				continue
			}

			if tx := txByHash[vLog.TxHash]; tx != nil {
				sender, err := senderFromTx(tx, i.chainID)
				if err == nil {
					swapMsg.To = sender
				} else {
					log.Printf("[warn] failed to derive sender for tx %s: %v", vLog.TxHash.Hex(), err)
				}
			} else {
				log.Printf("[warn] missing transaction data for %s; keeping log-derived recipient", vLog.TxHash.Hex())
			}

			if err := i.publishSwap(ctx, swapMsg); err != nil {
				log.Printf("[warn] failed to publish pancake swap %s: %v", swapMsg.TxHash.Hex(), err)
			}
		}
	}

	lastNumber := headers[len(headers)-1].Number.Uint64()
	if i.updateLastProcessed(lastNumber) {
		i.persistCheckpoint(ctx)
	}

	if skippedLogs > 0 {
		log.Printf("[info] processed block batch %d-%d (%d logs, %d skipped due to missing blocks)", headers[0].Number.Uint64(), lastNumber, len(logs), skippedLogs)
	} else {
		log.Printf("[info] processed block batch %d-%d (%d logs)", headers[0].Number.Uint64(), lastNumber, len(logs))
	}
	return nil
}

// publishTrade sends a trade to the RabbitMQ trades queue.
func (i *Indexer) publishTrade(ctx context.Context, tradeMsg *trade.Trade) error {
	body, err := json.Marshal(tradeMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal trade: %w", err)
	}

	return i.amqpChannel.PublishWithContext(ctx, "", queue.TradesQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

// publishSwap sends a swap to the RabbitMQ pool swaps queue.
func (i *Indexer) publishSwap(ctx context.Context, swapMsg *trade.Swap) error {
	body, err := json.Marshal(swapMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal swap: %w", err)
	}

	return i.amqpChannel.PublishWithContext(ctx, "", queue.PoolSwapsQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

type archiveLogsPayload struct {
	From uint64      `json:"from"`
	To   uint64      `json:"to"`
	Logs []types.Log `json:"logs"`
}

type archiveBlockPayload struct {
	Number uint64 `json:"number"`
	RLP    string `json:"rlp"`
}

func (i *Indexer) publishArchiveLogs(ctx context.Context, from, to uint64, logs []types.Log) error {
	payload := archiveLogsPayload{
		From: from,
		To:   to,
		Logs: logs,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal archive logs payload: %w", err)
	}

	return i.amqpChannel.PublishWithContext(ctx, "", queue.ArchiveLogsQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func (i *Indexer) publishArchiveBlock(ctx context.Context, block *types.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}

	encoded, err := rlp.EncodeToBytes(block)
	if err != nil {
		return fmt.Errorf("failed to marshal block %d: %w", block.NumberU64(), err)
	}

	payload := archiveBlockPayload{
		Number: block.NumberU64(),
		RLP:    hex.EncodeToString(encoded),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal archive block payload: %w", err)
	}

	return i.amqpChannel.PublishWithContext(ctx, "", queue.ArchiveBlocksQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

// headerWithNumber creates a lightweight header containing only the block number.
func headerWithNumber(number uint64) *types.Header {
	return &types.Header{Number: new(big.Int).SetUint64(number)}
}

// senderFromTx derives the sender address from the signed transaction data.
func senderFromTx(tx *types.Transaction, defaultChainID *big.Int) (common.Address, error) {
	if tx == nil {
		return common.Address{}, errors.New("transaction is nil")
	}

	if chainID := tx.ChainId(); chainID != nil && chainID.Sign() != 0 {
		signer := types.LatestSignerForChainID(chainID)
		addr, err := types.Sender(signer, tx)
		if err != nil {
			return common.Address{}, err
		}
		return addr, nil
	}

	if defaultChainID != nil && defaultChainID.Sign() != 0 {
		signer := types.LatestSignerForChainID(defaultChainID)
		addr, err := types.Sender(signer, tx)
		if err != nil {
			return common.Address{}, err
		}
		return addr, nil
	}

	addr, err := types.Sender(types.HomesteadSigner{}, tx)
	if err != nil {
		return common.Address{}, err
	}
	return addr, nil
}

// Close ensures connections and checkpoints are persisted gracefully.
func (i *Indexer) Close(ctx context.Context) {
	log.Println("[info] shutting down blocks indexer...")
	i.persistCheckpoint(ctx)

	if err := i.amqpChannel.Close(); err != nil {
		log.Printf("[warn] failed to close amqp channel: %v", err)
	}
	if err := i.amqpConn.Close(); err != nil {
		log.Printf("[warn] failed to close amqp connection: %v", err)
	}
	if err := i.redisClient.Close(); err != nil {
		log.Printf("[warn] failed to close redis client: %v", err)
	}
	i.ethWSSClient.Close()
	i.ethHTTPClient.Close()
}

func main() {
	startFrom := flag.Uint64("startFrom", 0, "override the last processed block before startup (block number)")
	archiveEnabled := flag.Bool("archive", false, "publish raw blocks and logs to archive queues")
	flag.Parse()

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
	defer cancel()

	indexer, err := NewIndexer(wssURL, httpURL, rabbitmqURL, redisURL, *archiveEnabled)
	if err != nil {
		log.Fatalf("[fatal] failed to create indexer: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := indexer.Run(ctx, *startFrom); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("[fatal] indexer exited with error: %v", err)
		}
	}()

	<-sigChan
	log.Println("[info] received shutdown signal")
	indexer.Close(context.Background())
	cancel()
	wg.Wait()
	log.Println("[info] blocks indexer shut down cleanly")
}
