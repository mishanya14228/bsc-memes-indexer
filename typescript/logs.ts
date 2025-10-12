import { Interface, Log, Result, WebSocketProvider, getAddress } from "ethers";
import type { Filter } from "ethers";

const WSS_URL = "wss://bsc-mainnet.core.chainstack.com/744dc72d26917be15394b4a31037440f";

const ABI = [
  "event Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)"
];

const POOL_METADATA_ABI = [
  "function token0() view returns (address)",
  "function token1() view returns (address)"
];

const poolMetadataInterface = new Interface(POOL_METADATA_ABI);

type StructuredArgs = Record<string, unknown>;
type PoolTokens = { token0: string; token1: string };

const poolMetadataCache = new Map<string, PoolTokens>();
const poolMetadataInflight = new Map<string, Promise<PoolTokens>>();

function buildFilter(iface: Interface): Filter {
  const eventFragments = iface.fragments.filter((fragment) => fragment.type === "event");

  if (eventFragments.length === 0) {
    throw new Error("Provided ABI does not contain any event fragments");
  }

  const eventTopics = eventFragments.map((fragment) => fragment.topicHash);

  return {
    topics: [eventTopics.length === 1 ? eventTopics[0] : eventTopics]
  };
}

function normaliseValue(value: unknown): unknown {
  if (typeof value === "bigint") {
    return value.toString();
  }

  if (Array.isArray(value)) {
    return value.map((item) => normaliseValue(item));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entryValue]) => [key, normaliseValue(entryValue)])
    );
  }

  return value;
}

function formatArgs(result: Result): StructuredArgs {
  const namedArgs = result.toObject();

  return Object.fromEntries(
    Object.entries(namedArgs).map(([key, value]) => [key, normaliseValue(value)])
  );
}

async function loadPoolTokens(provider: WebSocketProvider, poolAddress: string): Promise<PoolTokens> {
  const key = poolAddress.toLowerCase();
  const cached = poolMetadataCache.get(key);
  if (cached) {
    return cached;
  }

  const existing = poolMetadataInflight.get(key);
  if (existing) {
    return existing;
  }

  const inflight = (async () => {
    try {
      const [rawToken0, rawToken1] = await Promise.all([
        provider.call({ to: poolAddress, data: poolMetadataInterface.encodeFunctionData("token0") }),
        provider.call({ to: poolAddress, data: poolMetadataInterface.encodeFunctionData("token1") })
      ]);

      const decoded0 = poolMetadataInterface.decodeFunctionResult("token0", rawToken0)[0];
      const decoded1 = poolMetadataInterface.decodeFunctionResult("token1", rawToken1)[0];

      const tokens = {
        token0: getAddress(decoded0),
        token1: getAddress(decoded1)
      } as PoolTokens;

      poolMetadataCache.set(key, tokens);

      return tokens;
    } finally {
      poolMetadataInflight.delete(key);
    }
  })();

  poolMetadataInflight.set(key, inflight);

  return inflight;
}

async function main(): Promise<void> {
  const iface = new Interface(ABI);
  const eventCount = iface.fragments.filter((fragment) => fragment.type === "event").length;
  const filter = buildFilter(iface);
  const provider = new WebSocketProvider(WSS_URL);

  try {
    provider.websocket.onerror = (error) => {
      console.error("[websocket:error]", error);
    };
  } catch (error) {
    console.error("[warn] Unable to attach websocket error handler", error);
  }

  console.error(`[info] subscribing to ${eventCount} event fragment(s)`);

  provider.on(filter, async (log: Log) => {
    try {
      const parsed = iface.parseLog(log);
      // const tokens = await loadPoolTokens(provider, log.address);
      const payload = {
        event: parsed.name,
        signature: parsed.signature,
        address: log.address,
        txHash: log.transactionHash,
        blockNumber: log.blockNumber,
        // token0: tokens.token0,
        // token1: tokens.token1,
        args: formatArgs(parsed.args)
      };

      console.log(JSON.stringify(payload));
    } catch (error) {
      console.error("[warn] Failed to process log", error, log);
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
