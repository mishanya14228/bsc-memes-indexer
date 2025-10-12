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

	"bsc-memes-indexer/shared/contracts"
	"bsc-memes-indexer/shared/topics"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

var (
	dataTypes abi.Arguments
)

func init() {
	mustType := func(t string) abi.Type {
		ty, err := abi.NewType(t, "", nil)
		if err != nil {
			panic(err)
		}
		return ty
	}

	dataTypes = abi.Arguments{
		{Type: mustType("address")}, // token
		{Type: mustType("address")}, // trader
		{Type: mustType("uint256")},
		{Type: mustType("uint256")}, // tokens count
		{Type: mustType("uint256")}, // bnb count
		{Type: mustType("uint256")},
		{Type: mustType("uint256")},
		{Type: mustType("uint256")},
	}
}

// Trade represents a decoded buy or sell event.
type Trade struct {
	TxHash       common.Hash    `json:"tx"`
	Direction    string         `json:"direction"`
	Token        common.Address `json:"token"`
	Trader       common.Address `json:"trader"`
	TokensAmount *big.Int       `json:"tokensAmount"`
	BnbAmount    *big.Int       `json:"bnbAmount"`
}

// Producer handles the connection and subscription to the blockchain.
type Producer struct {
	client   *ethclient.Client
	contract common.Address
}

// NewProducer creates and returns a new Producer.
func NewProducer(wssURL string) (*Producer, error) {
	client, err := ethclient.Dial(wssURL)
	if err != nil {
		return nil, err
	}

	return &Producer{
		client:   client,
		contract: common.HexToAddress(contracts.FourMemeContractAddress),
	}, nil
}

// Start begins the log subscription and returns channels for trades and errors.
func (p *Producer) Start(ctx context.Context) (<-chan *Trade, <-chan error) {
	logs := make(chan types.Log)
	trades := make(chan *Trade)
	errChan := make(chan error, 1)

	query := ethereum.FilterQuery{
		Addresses: []common.Address{p.contract},
		Topics:    [][]common.Hash{{topics.FourMemeBuyTopic, topics.FourMemeSellTopic}},
	}

	sub, err := p.client.SubscribeFilterLogs(ctx, query, logs)
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
				trade, err := p.processLog(vLog)
				if err != nil {
					log.Printf("[warn] failed to process log: %v", err)
					continue
				}
				trades <- trade
			}
		}
	}()

	return trades, errChan
}

// Close terminates the producer's connection.
func (p *Producer) Close() {
	p.client.Close()
}

func (p *Producer) processLog(vLog types.Log) (*Trade, error) {
	direction := "sell"
	if vLog.Topics[0] == topics.FourMemeBuyTopic {
		direction = "buy"
	}

	unpackedData, err := dataTypes.Unpack(vLog.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack log data for tx %s: %w", vLog.TxHash.Hex(), err)
	}

	trade := &Trade{
		TxHash:       vLog.TxHash,
		Direction:    direction,
		Token:        unpackedData[0].(common.Address),
		Trader:       unpackedData[1].(common.Address),
		TokensAmount: unpackedData[3].(*big.Int),
		BnbAmount:    unpackedData[4].(*big.Int),
	}

	return trade, nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("[info] .env file not found, relying on environment variables")
	}

	wssURL := os.Getenv("BSC_WSS_URL")

	if wssURL == "" {
		log.Fatalf("[fatal] BSC_WSS_URL must be set")
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

	producer, err := NewProducer(wssURL)
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
			jsonOutput, err := json.Marshal(trade)
			if err != nil {
				log.Printf("[warn] failed to marshal trade to JSON: %v", err)
				continue
			}
			log.Println(string(jsonOutput))
		}
	}
}
