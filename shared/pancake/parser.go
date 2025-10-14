package pancake

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	pancakeabi "github.com/mikhailzakipniy/bsc-memes-indexer/shared/abi"
	"github.com/mikhailzakipniy/bsc-memes-indexer/shared/trade"
)

var swapABI abi.ABI

func init() {
	var err error
	swapABI, err = abi.JSON(strings.NewReader(pancakeabi.PancakeSwapPairABI))
	if err != nil {
		panic(fmt.Sprintf("failed to parse pancake ABI: %v", err))
	}
}

// SwapEventTopic returns the topic hash for PancakeSwap swap events.
func SwapEventTopic() common.Hash {
	return swapABI.Events["Swap"].ID
}

// ParseSwapFromLog converts a PancakeSwap log into the shared swap structure.
func ParseSwapFromLog(vLog types.Log, timestamp uint64) (*trade.Swap, error) {
	unpackedData, err := swapABI.Events["Swap"].Inputs.NonIndexed().Unpack(vLog.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack log data: %w", err)
	}

	if len(vLog.Topics) < 3 {
		return nil, fmt.Errorf("invalid swap event: expected at least 3 topics, got %d", len(vLog.Topics))
	}

	senderAddress := common.BytesToAddress(vLog.Topics[1].Bytes())
	toAddress := common.BytesToAddress(vLog.Topics[2].Bytes())

	amount0In, ok := unpackedData[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse amount0In for tx %s", vLog.TxHash.Hex())
	}
	amount1In, ok := unpackedData[1].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse amount1In for tx %s", vLog.TxHash.Hex())
	}
	amount0Out, ok := unpackedData[2].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse amount0Out for tx %s", vLog.TxHash.Hex())
	}
	amount1Out, ok := unpackedData[3].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse amount1Out for tx %s", vLog.TxHash.Hex())
	}

	return &trade.Swap{
		PoolAddress: vLog.Address,
		Block:       vLog.BlockNumber,
		Timestamp:   timestamp,
		TxHash:      vLog.TxHash,
		Sender:      senderAddress,
		To:          toAddress,
		Amount0In:   amount0In,
		Amount1In:   amount1In,
		Amount0Out:  amount0Out,
		Amount1Out:  amount1Out,
	}, nil
}
