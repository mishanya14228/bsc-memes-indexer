package trade

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// Trade represents a decoded buy or sell event.
type Trade struct {
	Platform     string         `json:"platform"`
	Block        uint64         `json:"block"`
	Timestamp    uint64         `json:"timestamp"`
	TxHash       common.Hash    `json:"tx"`
	Direction    string         `json:"direction"`
	Token        common.Address `json:"token"`
	Trader       common.Address `json:"trader"`
	TokensAmount *big.Int       `json:"tokensAmount"`
	BnbAmount    *big.Int       `json:"bnbAmount"`
}
