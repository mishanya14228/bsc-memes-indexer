package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	pancakeabi "github.com/mikhailzakipniy/bsc-memes-indexer/shared/abi"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/queue"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/tokens"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/trade"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	platformName      = "Pancake"
	maxRetries        = 5
	initialRetryDelay = 1 * time.Second
)

var (
	pancakePoolABI abi.ABI
	tokenABI       abi.ABI
)

func init() {
	var err error
	pancakePoolABI, err = abi.JSON(strings.NewReader(pancakeabi.PancakeSwapPairABI))
	if err != nil {
		panic(fmt.Sprintf("failed to parse pancake pair ABI: %v", err))
	}
	// ABI for token0() and token1() functions
	tokenABI, err = abi.JSON(strings.NewReader(`[{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("failed to parse token ABI: %v", err))
	}
}

// Relay handles connections and the core relaying logic.
type Relay struct {
	ethClient   *ethclient.Client
	redisClient *redis.Client
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
}

// NewRelay creates a new Relay instance.
func NewRelay(rpcURL, rabbitmqURL, redisURL string) (*Relay, error) {
	ethClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial eth client: %w", err)
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

	// Declare the queue we are consuming from
	_, err = amqpChannel.QueueDeclare(queue.PoolSwapsQueue, true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to declare consumer queue: %w", err)
	}

	// Declare the queue we are producing to
	_, err = amqpChannel.QueueDeclare(queue.TradesQueue, true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to declare producer queue: %w", err)
	}

	return &Relay{
		ethClient:   ethClient,
		redisClient: redisClient,
		amqpConn:    amqpConn,
		amqpChannel: amqpChannel,
	}, nil
}

// loadPoolTokens fetches token addresses for a pool, with caching and retries.
func (r *Relay) loadPoolTokens(ctx context.Context, poolAddress common.Address) (common.Address, common.Address, error) {
	cacheKey := fmt.Sprintf("pool:%s", poolAddress.Hex())

	// Check cache first
	cached, err := r.redisClient.HGetAll(ctx, cacheKey).Result()
	if err == nil && cached["token0"] != "" && cached["token1"] != "" {
		return common.HexToAddress(cached["token0"]), common.HexToAddress(cached["token1"]), nil
	}

	// If not in cache, fetch from blockchain with retry logic
	token0Data, err := tokenABI.Pack("token0")
	if err != nil {
		return common.Address{}, common.Address{}, err
	}
	token1Data, err := tokenABI.Pack("token1")
	if err != nil {
		return common.Address{}, common.Address{}, err
	}

	var token0, token1 common.Address
	var rawToken0, rawToken1 []byte
	delay := initialRetryDelay

	for i := 0; i < maxRetries; i++ {
		rawToken0, err = r.ethClient.CallContract(ctx, ethereum.CallMsg{To: &poolAddress, Data: token0Data}, nil)
		if err == nil {
			break
		}
		log.Printf("[warn] failed to get token0 (attempt %d/%d): %v. Retrying in %v...", i+1, maxRetries, err, delay)
		time.Sleep(delay)
		delay *= 2
	}
	if err != nil {
		return common.Address{}, common.Address{}, fmt.Errorf("failed to get token0 after %d retries: %w", maxRetries, err)
	}

	delay = initialRetryDelay
	for i := 0; i < maxRetries; i++ {
		rawToken1, err = r.ethClient.CallContract(ctx, ethereum.CallMsg{To: &poolAddress, Data: token1Data}, nil)
		if err == nil {
			break
		}
		log.Printf("[warn] failed to get token1 (attempt %d/%d): %v. Retrying in %v...", i+1, maxRetries, err, delay)
		time.Sleep(delay)
		delay *= 2
	}
	if err != nil {
		return common.Address{}, common.Address{}, fmt.Errorf("failed to get token1 after %d retries: %w", maxRetries, err)
	}

	token0 = common.BytesToAddress(rawToken0)
	token1 = common.BytesToAddress(rawToken1)

	// Cache the result
	pipe := r.redisClient.Pipeline()
	pipe.HSet(ctx, cacheKey, "token0", token0.Hex())
	pipe.HSet(ctx, cacheKey, "token1", token1.Hex())
	pipe.Expire(ctx, cacheKey, 10*24*time.Hour)
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("[warn] failed to cache pool tokens for %s: %v", poolAddress.Hex(), err)
	}

	return token0, token1, nil
}

// processAndRelaySwap is the core business logic for the relay.
func (r *Relay) processAndRelaySwap(ctx context.Context, swap *trade.Swap) error {
	token0, token1, err := r.loadPoolTokens(ctx, swap.PoolAddress)
	if err != nil {
		return fmt.Errorf("could not load pool tokens: %w", err)
	}

	// Filter for pairs that include WBNB
	var memeToken common.Address
	var bnbAmount, tokenAmount *big.Int
	var direction string

	if token0 == tokens.WBNBAddress {
		memeToken = token1
		if swap.Amount0In.Cmp(big.NewInt(0)) > 0 && swap.Amount1Out.Cmp(big.NewInt(0)) > 0 { // Spending WBNB to get meme token
			direction = "BUY"
			bnbAmount = swap.Amount0In
			tokenAmount = swap.Amount1Out
		} else { // Spending meme token to get WBNB
			direction = "SELL"
			bnbAmount = swap.Amount0Out
			tokenAmount = swap.Amount1In
		}
	} else if token1 == tokens.WBNBAddress {
		memeToken = token0
		if swap.Amount1In.Cmp(big.NewInt(0)) > 0 && swap.Amount0Out.Cmp(big.NewInt(0)) > 0 { // Spending WBNB to get meme token
			direction = "BUY"
			bnbAmount = swap.Amount1In
			tokenAmount = swap.Amount0Out
		} else { // Spending meme token to get WBNB
			direction = "SELL"
			bnbAmount = swap.Amount1Out
			tokenAmount = swap.Amount0In
		}
	} else {
		// Not a WBNB pair, ignore
		return nil
	}

	finalTrade := &trade.Trade{
		Platform:     platformName,
		Block:        swap.Block,
		Timestamp:    swap.Timestamp,
		TxHash:       swap.TxHash,
		Direction:    direction,
		Token:        memeToken,
		Trader:       swap.To, // The ultimate recipient of the tokens
		TokensAmount: tokenAmount,
		BnbAmount:    bnbAmount,
	}

	// Publish the transformed trade
	body, err := json.Marshal(finalTrade)
	if err != nil {
		return fmt.Errorf("failed to marshal final trade: %w", err)
	}

	return r.amqpChannel.PublishWithContext(ctx, "", queue.TradesQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[info] .env file not found, relying on environment variables")
	}

	rpcURL := os.Getenv("BSC_RPC_URL")
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	redisURL := os.Getenv("REDIS_URL")
	if rpcURL == "" || rabbitmqURL == "" || redisURL == "" {
		log.Fatalf("[fatal] BSC_RPC_URL, RABBITMQ_URL, and REDIS_URL must be set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	relay, err := NewRelay(rpcURL, rabbitmqURL, redisURL)
	if err != nil {
		log.Fatalf("[fatal] failed to create relay: %v", err)
	}
	defer relay.amqpConn.Close()
	defer relay.amqpChannel.Close()
	defer relay.redisClient.Close()

	msgs, err := relay.amqpChannel.Consume(queue.PoolSwapsQueue, "pancake-relay", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("[fatal] failed to register a consumer: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("[info] received signal, shutting down...")
		cancel()
	}()

	log.Println("[info] starting pancake relay... waiting for messages.")
	for {
		select {
		case <-ctx.Done():
			log.Println("[info] shutting down relay loop.")
			return
		case d, ok := <-msgs:
			if !ok {
				log.Println("[info] consumer channel closed.")
				return
			}

			var swap trade.Swap
			if err := json.Unmarshal(d.Body, &swap); err != nil {
				log.Printf("[warn] failed to unmarshal swap message: %v", err)
				continue
			}

			if err := relay.processAndRelaySwap(ctx, &swap); err != nil {
				log.Printf("[warn] failed to process and relay swap: %v", err)
			}
		}
	}
}
