import { Interface, Log, Result, WebSocketProvider, getAddress } from "ethers";

const POOL_METADATA_ABI = [
    "function token0() view returns (address)",
    "function token1() view returns (address)"
];

const poolMetadataInterface = new Interface(POOL_METADATA_ABI);

type PoolTokens = { token0: string; token1: string };

const poolMetadataCache = new Map<string, PoolTokens>();
const poolMetadataInflight = new Map<string, Promise<PoolTokens>>();

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