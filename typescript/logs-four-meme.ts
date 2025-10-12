import {AbiCoder, Log, WebSocketProvider} from "ethers";

const WSS_URL = "wss://bsc-mainnet.core.chainstack.com/744dc72d26917be15394b4a31037440f";
const CONTRACT_ADDRESS = "0x5c952063c7fc8610ffdb798152d69f0b9550762b";

async function main(): Promise<void> {
  const provider = new WebSocketProvider(WSS_URL);

  try {
    provider.websocket.onerror = (error) => {
      console.error("[websocket:error]", error);
    };
  } catch (error) {
    console.error("[warn] Unable to attach websocket error handler", error);
  }

  const allowedTopics = [
    "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942",
    "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19"
  ];

  console.error(`[info] subscribing to logs for ${CONTRACT_ADDRESS} with ${allowedTopics.length} topic filter(s)`);

  provider.on({address: CONTRACT_ADDRESS, topics: [allowedTopics]}, (log: Log) => {
    /**
     * topic buy -> 0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942
     * 1 -> token address
     * 2 -> wallet address
     * 3 -> hz
     * 4 -> tokens count?
     * 5 -> bnb count?
     *
     *
     * topic sell -> 0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19
     * same positions
     * */
    try {
      // const payload = {
      //   address: log.address,
      //   txHash: log.transactionHash,
      //   blockNumber: log.blockNumber,
      //   topics: log.topics,
      //   data: log.data
      // };
      const decoded = AbiCoder.defaultAbiCoder().decode(
        [
          "address", // token
          "address", // trader
          "uint256",
          "uint256", // tokens count
          "uint256", // bnb count
          "uint256",
          "uint256",
          "uint256"
        ],
        log.data
      );

      const [token, trader, _, tokensAmount, bnbAmount] = decoded;

      const direction = log.topics[0] === allowedTopics[0] ? 'buy': 'sell';
      console.log({tx: log.transactionHash, direction, token, trader, tokensAmount, bnbAmount })
      // console.log(`${log.transactionHash}::${log.topics}::${direction}::${token}::${trader}::$`)

      // console.log(JSON.stringify(payload));
    } catch (error) {
      console.error("[warn] Failed to serialise log", error, log);
    }
  });

  process.on("SIGINT", async () => {
    console.error("[info] received SIGINT, closing websocket...");
    try {
      await provider.destroy();
    } catch (error) {
      console.error("[warn] error while closing provider", error);
    } finally {
      process.exit(0);
    }
  });
}

main().catch((error) => {
  console.error("[fatal] subscription setup failed", error);
  process.exit(1);
});
