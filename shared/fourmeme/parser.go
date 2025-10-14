package fourmeme

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/topics"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/trade"
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

// ParseTradeFromLog converts a Four.Meme log into the canonical trade format.
func ParseTradeFromLog(vLog types.Log, timestamp uint64) (*trade.Trade, error) {
	direction := "sell"
	if len(vLog.Topics) > 0 && vLog.Topics[0] == topics.FourMemeBuyTopic {
		direction = "buy"
	}

	unpackedData, err := dataTypes.Unpack(vLog.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack log data for tx %s: %w", vLog.TxHash.Hex(), err)
	}

	token, ok := unpackedData[0].(common.Address)
	if !ok {
		return nil, fmt.Errorf("failed to parse token address for tx %s", vLog.TxHash.Hex())
	}
	trader, ok := unpackedData[1].(common.Address)
	if !ok {
		return nil, fmt.Errorf("failed to parse trader address for tx %s", vLog.TxHash.Hex())
	}
	tokensAmount, ok := unpackedData[3].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse tokens amount for tx %s", vLog.TxHash.Hex())
	}
	bnbAmount, ok := unpackedData[4].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse bnb amount for tx %s", vLog.TxHash.Hex())
	}

	return &trade.Trade{
		Platform:     "four.meme",
		Block:        vLog.BlockNumber,
		Timestamp:    timestamp,
		TxHash:       vLog.TxHash,
		Direction:    direction,
		Token:        token,
		Trader:       trader,
		TokensAmount: tokensAmount,
		BnbAmount:    bnbAmount,
	}, nil
}
