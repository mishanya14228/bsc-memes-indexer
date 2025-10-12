package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bsc-memes-indexer/shared/contracts"
	"bsc-memes-indexer/shared/queue"
	"bsc-memes-indexer/shared/topics"
	"bsc-memes-indexer/shared/trade"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

var dataTypes abi.Arguments

func init() {
	mustType := func(t string) abi.Type {
		ty, err := abi.NewType(t, "", nil)
		if err != nil {
			panic(fmt.Sprintf("failed to create type: %v", err))
		}
		return ty
	}
	dataTypes = abi.Arguments{
		{Type: mustType("address")}, {Type: mustType("address")}, {Type: mustType("uint256")},
		{Type: mustType("uint256")}, {Type: mustType("uint256")}, {Type: mustType("uint256")},
		{Type: mustType("uint256")}, {Type: mustType("uint256")},
	}
}

// Producer handles the connection and subscription to the blockchain and RabbitMQ.
type Producer struct {
	ethClient   *ethclient.Client
	contract    common.Address
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
}

// NewProducer creates and returns a new Producer.
func NewProducer(wssURL, rabbitmqURL string) (*Producer, error) {
	ethClient, err := ethclient.Dial(wssURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial eth client: %w", err)
	}

	amqpConn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to dial amqp: %w", err)
	}

	amqpChannel, err := amqpConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open amqp channel: %w", err)
	}

	_, err = amqpChannel.QueueDeclare(
		queue.TradesQueue, // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	return &Producer{
		ethClient:   ethClient,
		contract:    common.HexToAddress(contracts.FourMemeContractAddress),
		amqpConn:    amqpConn,
		amqpChannel: amqpChannel,
	}, nil
}

// Start begins the log subscription and returns channels for trades and errors.
func (p *Producer) Start(ctx context.Context) (<-chan *trade.Trade, <-chan error) {
	logs := make(chan types.Log)
	trades := make(chan *trade.Trade)
	errChan := make(chan error, 1)

	query := ethereum.FilterQuery{
		Addresses: []common.Address{p.contract},
		Topics:    [][]common.Hash{{topics.FourMemeBuyTopic, topics.FourMemeSellTopic}},
	}

	sub, err := p.ethClient.SubscribeFilterLogs(ctx, query, logs)
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
				processedTrade, err := p.processLog(ctx, vLog)
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

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return p.amqpChannel.PublishWithContext(ctx,
		"",                // exchange
		queue.TradesQueue, // routing key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

// Close terminates all connections.
func (p *Producer) Close() {
	p.ethClient.Close()
	p.amqpChannel.Close()
	p.amqpConn.Close()
}

func (p *Producer) processLog(ctx context.Context, vLog types.Log) (*trade.Trade, error) {
	header, err := p.ethClient.HeaderByNumber(ctx, big.NewInt(int64(vLog.BlockNumber)))
	if err != nil {
		return nil, fmt.Errorf("failed to get block header: %w", err)
	}

	direction := "sell"
	if vLog.Topics[0] == topics.FourMemeBuyTopic {
		direction = "buy"
	}

	unpackedData, err := dataTypes.Unpack(vLog.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack log data for tx %s: %w", vLog.TxHash.Hex(), err)
	}

	return &trade.Trade{
		Platform:     "four.meme",
		Block:        vLog.BlockNumber,
		Timestamp:    header.Time,
		TxHash:       vLog.TxHash,
		Direction:    direction,
		Token:        unpackedData[0].(common.Address),
		Trader:       unpackedData[1].(common.Address),
		TokensAmount: unpackedData[3].(*big.Int),
		BnbAmount:    unpackedData[4].(*big.Int),
	}, nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("[info] .env file not found, relying on environment variables")
	}

	wssURL := os.Getenv("BSC_WSS_URL")
	rabbitmqURL := os.Getenv("RABBITMQ_URL")

	if wssURL == "" || rabbitmqURL == "" {
		log.Fatalf("[fatal] BSC_WSS_URL and RABBITMQ_URL must be set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("[info] received signal, shutting down...")
		cancel()
	}()

	producer, err := NewProducer(wssURL, rabbitmqURL)
	if err != nil {
		log.Fatalf("[fatal] failed to create producer: %v", err)
	}
	defer producer.Close()

	trades, errChan := producer.Start(ctx)

	log.Println("[info] starting producer...")

	for {
		select {
		case <-ctx.Done():
			log.Println("[info] shutting down producer loop.")
			return
		case err, ok := <-errChan:
			if !ok {
				log.Println("[info] error channel closed.")
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
			if err := producer.Publish(ctx, trade); err != nil {
				log.Printf("[warn] failed to publish trade: %v", err)
			} else {
				log.Printf("[info] published trade %s", trade.TxHash.Hex())
			}
		}
	}
}
