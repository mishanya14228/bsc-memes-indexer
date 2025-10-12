package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pancakeabi "bsc-memes-indexer/shared/abi"
	"bsc-memes-indexer/shared/queue"
	"bsc-memes-indexer/shared/trade"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

var pancakeABI abi.ABI

func init() {
	var err error
	pancakeABI, err = abi.JSON(strings.NewReader(pancakeabi.PancakeSwapPairABI))
	if err != nil {
		panic(fmt.Sprintf("failed to parse pancake ABI: %v", err))
	}
}

// Producer handles the connection and subscription to the blockchain and RabbitMQ.
type Producer struct {
	ethClient   *ethclient.Client
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
		queue.PoolSwapsQueue, // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	return &Producer{
		ethClient:   ethClient,
		amqpConn:    amqpConn,
		amqpChannel: amqpChannel,
	}, nil
}

// Start begins the log subscription and returns channels for swaps and errors.
func (p *Producer) Start(ctx context.Context) (<-chan *trade.Swap, <-chan error) {
	logs := make(chan types.Log)
	swaps := make(chan *trade.Swap)
	errChan := make(chan error, 1)

	query := ethereum.FilterQuery{
		Topics: [][]common.Hash{{pancakeABI.Events["Swap"].ID}},
	}

	sub, err := p.ethClient.SubscribeFilterLogs(ctx, query, logs)
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
				processedSwap, err := p.processLog(ctx, vLog)
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

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return p.amqpChannel.PublishWithContext(ctx,
		"",                   // exchange
		queue.PoolSwapsQueue, // routing key
		false,                // mandatory
		false,                // immediate
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

func (p *Producer) processLog(ctx context.Context, vLog types.Log) (*trade.Swap, error) {
	header, err := p.ethClient.HeaderByNumber(ctx, big.NewInt(int64(vLog.BlockNumber)))
	if err != nil {
		return nil, fmt.Errorf("failed to get block header: %w", err)
	}

	// The non-indexed arguments are packed into the Data field.
	// We need to unpack them according to the ABI
	unpackedData, err := pancakeABI.Events["Swap"].Inputs.NonIndexed().Unpack(vLog.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack log data: %w", err)
	}

	// The indexed fields are stored in the Topics.
	// Topic[0] is the event signature hash, subsequent topics are the indexed fields.
	if len(vLog.Topics) < 3 {
		return nil, fmt.Errorf("invalid swap event: expected at least 3 topics, got %d", len(vLog.Topics))
	}

	senderAddress := common.BytesToAddress(vLog.Topics[1].Bytes())
	toAddress := common.BytesToAddress(vLog.Topics[2].Bytes())

	// Map the unpacked non-indexed data to the correct fields.
	// The order is defined by the ABI: amount0In, amount1In, amount0Out, amount1Out
	amount0In := unpackedData[0].(*big.Int)
	amount1In := unpackedData[1].(*big.Int)
	amount0Out := unpackedData[2].(*big.Int)
	amount1Out := unpackedData[3].(*big.Int)

	return &trade.Swap{
		PoolAddress: vLog.Address,
		Block:       vLog.BlockNumber,
		Timestamp:   header.Time,
		TxHash:      vLog.TxHash,
		Sender:      senderAddress,
		To:          toAddress,
		Amount0In:   amount0In,
		Amount1In:   amount1In,
		Amount0Out:  amount0Out,
		Amount1Out:  amount1Out,
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

	swaps, errChan := producer.Start(ctx)

	log.Println("[info] starting pancake swap producer...")

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
		case swap, ok := <-swaps:
			if !ok {
				log.Println("[info] swaps channel closed.")
				return
			}
			if err := producer.Publish(ctx, swap); err != nil {
				log.Printf("[warn] failed to publish swap: %v", err)
			} else {
				log.Printf("[info] published swap from pool %s in tx %s", swap.PoolAddress.Hex(), swap.TxHash.Hex())
			}
		}
	}
}
