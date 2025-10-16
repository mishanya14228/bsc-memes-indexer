package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"sync"
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
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/pancake"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/queue"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/redis_keys"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/topics"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/trade"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	blockBatchSize      = 10
	retryDelayOnFailure = 3 * time.Second
)

// Indexer coordinates block streaming, log filtering, and publishing.
type Indexer struct {
	ethWSSClient  *ethclient.Client
	ethHTTPClient *ethclient.Client
	redisClient   *redis.Client
	amqpConn      *amqp.Connection
	amqpChannel   *amqp.Channel

	lastProcessedBlock atomic.Uint64
}

// NewIndexer wires dependencies and declares required queues.
func NewIndexer(wssURL, httpURL, rabbitmqURL, redisURL string) (*Indexer, error) {
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

	dlxName := "pool-swaps-dlx"
	if err := amqpChannel.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("failed to declare dlx: %w", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange": dlxName,
	}
	if _, err := amqpChannel.QueueDeclare(queue.PoolSwapsQueue, true, false, false, false, args); err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %w", queue.PoolSwapsQueue, err)
	}

	return &Indexer{
		ethWSSClient:  ethWSSClient,
		ethHTTPClient: ethHTTPClient,
		redisClient:   redisClient,
		amqpConn:      amqpConn,
		amqpChannel:   amqpChannel,
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
	} else {
		log.Printf("[info] persisted checkpoint at block %d", last)
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
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		latest, err := i.ethHTTPClient.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("failed to get latest block number: %w", err)
		}

		next := i.lastProcessedBlock.Load() + 1
		if next > latest {
			return nil
		}

		end := next + blockBatchSize - 1
		if end > latest {
			end = latest
		}

		headers := make([]*types.Header, 0, end-next+1)
		for number := next; number <= end; number++ {
			headers = append(headers, headerWithNumber(number))
		}

		log.Printf("[info] historical catch-up processing blocks %d-%d", next, end)
		if err := i.processBatch(ctx, headers); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.Printf("[error] failed to process historical batch %d-%d: %v", next, end, err)
			time.Sleep(retryDelayOnFailure)
			continue
		}
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

	buffer := make([]*types.Header, 0, blockBatchSize)

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
			if header.Number == nil || header.Number.Uint64() <= last {
				continue
			}

			buffer = append(buffer, header)
			for len(buffer) >= blockBatchSize {
				batch := make([]*types.Header, blockBatchSize)
				copy(batch, buffer[:blockBatchSize])

				if err := i.processBatch(ctx, batch); err != nil {
					if errors.Is(err, context.Canceled) {
						return err
					}
					log.Printf("[error] failed to process live block batch %d-%d: %v", batch[0].Number.Uint64(), batch[len(batch)-1].Number.Uint64(), err)
					time.Sleep(retryDelayOnFailure)
					break
				}

				buffer = buffer[blockBatchSize:]
			}
		}
	}
}

// processBatch fetches logs and blocks for the provided headers, publishing any matching events.
func (i *Indexer) processBatch(ctx context.Context, headers []*types.Header) error {
	if len(headers) == 0 {
		return nil
	}

	from := headers[0].Number
	to := headers[len(headers)-1].Number

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

	blocks := make(map[uint64]*types.Block, len(headers))
	txByHash := make(map[common.Hash]*types.Transaction)
	for _, header := range headers {
		block, err := i.ethHTTPClient.BlockByNumber(ctx, header.Number)
		if err != nil {
			return fmt.Errorf("failed to fetch block %s: %w", header.Number.String(), err)
		}
		blocks[block.NumberU64()] = block
		for _, tx := range block.Transactions() {
			txByHash[tx.Hash()] = tx
		}
	}

	fourMemeAddress := common.HexToAddress(contracts.FourMemeContractAddress)
	swapTopic := pancake.SwapEventTopic()

	for _, vLog := range logs {
		block := blocks[vLog.BlockNumber]
		if block == nil {
			log.Printf("[warn] missing block data for number %d", vLog.BlockNumber)
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

			originalTo := swapMsg.To
			if tx := txByHash[vLog.TxHash]; tx != nil {
				sender, err := senderFromTx(tx)
				if err != nil {
					log.Printf("[debug] failed to derive sender for tx %s: %v; keeping log-derived To %s", vLog.TxHash.Hex(), err, originalTo.Hex())
				} else {
					swapMsg.To = sender
				}
			} else {
				log.Printf("[debug] missing transaction data for %s, keeping log-derived To %s", vLog.TxHash.Hex(), originalTo.Hex())
			}

			if err := i.publishSwap(ctx, swapMsg); err != nil {
				log.Printf("[warn] failed to publish pancake swap %s: %v", swapMsg.TxHash.Hex(), err)
			}
		}
	}

	lastNumber := headers[len(headers)-1].Number.Uint64()
	i.lastProcessedBlock.Store(lastNumber)
	i.persistCheckpoint(ctx)

	log.Printf("[info] processed block batch %d-%d (%d logs)", headers[0].Number.Uint64(), lastNumber, len(logs))
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

// headerWithNumber creates a lightweight header containing only the block number.
func headerWithNumber(number uint64) *types.Header {
	return &types.Header{Number: new(big.Int).SetUint64(number)}
}

// senderFromTx derives the sender address from the signed transaction data.
func senderFromTx(tx *types.Transaction) (common.Address, error) {
	if tx == nil {
		return common.Address{}, errors.New("transaction is nil")
	}

	if chainID := tx.ChainId(); chainID != nil {
		signer := types.LatestSignerForChainID(chainID)
		return types.Sender(signer, tx)
	}

	return types.Sender(types.HomesteadSigner{}, tx)
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

	indexer, err := NewIndexer(wssURL, httpURL, rabbitmqURL, redisURL)
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
			log.Printf("[error] indexer exited with error: %v", err)
			cancel()
		}
	}()

	<-sigChan
	log.Println("[info] received shutdown signal")
	indexer.Close(context.Background())
	cancel()
	wg.Wait()
	log.Println("[info] blocks indexer shut down cleanly")
}
