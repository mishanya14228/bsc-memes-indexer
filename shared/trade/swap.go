package trade

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// Swap represents a swap event from a DEX liquidity pool.
type Swap struct {
	PoolAddress common.Address `json:"poolAddress"`
	Block       uint64         `json:"block"`
	Timestamp   uint64         `json:"timestamp"`
	TxHash      common.Hash    `json:"txHash"`
	Sender      common.Address `json:"sender"`
	To          common.Address `json:"to"`
	Amount0In   *big.Int       `json:"amount0In"`
	Amount1In   *big.Int       `json:"amount1In"`
	Amount0Out  *big.Int       `json:"amount0Out"`
	Amount1Out  *big.Int       `json:"amount1Out"`
}
