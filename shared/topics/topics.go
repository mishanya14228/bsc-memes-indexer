package topics

import "github.com/ethereum/go-ethereum/common"

// Using vars so they are addressable, allowing them to be used in slices.
var (
	FourMemeBuyTopic  = common.HexToHash("0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942")
	FourMemeSellTopic = common.HexToHash("0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19")
)
