# Project Summary: bsc-memes-indexer

This document provides a high-level overview of the project architecture, services, and data flow.

## Architecture

This project follows a microservices architecture designed to capture cryptocurrency swap events from the Binance Smart Chain (BSC), process them, and standardize them into a single message queue.

The system is orchestrated using Docker Compose and consists of several Go applications that communicate via a RabbitMQ message broker. Redis is used for caching blockchain data to minimize RPC calls.

### Data Flow

1.  A single `blocks-indexer` service streams confirmed blocks from BSC.
    *   Blocks are processed in batches (configurable size and concurrency) with automatic progress tracking and gap backfilling.
    *   Every block batch is scanned for Four.Meme and Pancake swap events.
2.  The indexer publishes Four.Meme trades directly to the `trades` queue and generic Pancake swaps to the `pool-swaps` queue.
3.  (Optional) When started with `-archive`, the indexer also publishes raw block bodies (`archive-blocks`) and log batches (`archive-logs`) so the raw data can be replayed later without re-querying RPC.
4.  The `pancake-relay` service consumes messages from the generic `pool-swaps` queue.
5.  The relay enriches these messages by fetching token metadata (with Redis caching), filters for pairs involving WBNB, and transforms the data into a standardized `Trade` format.
6.  The relay then publishes this standardized `Trade` message to the final `trades` queue.

The final result is a single, clean stream of `Trade` messages in the `trades` queue, ready for consumption.

## Services

### `blocks-indexer`
-   **Source:** Subscribes to confirmed BSC blocks via WebSocket and periodically backfills any missed heights.
-   **Logic:**
    1.  Batches block headers (size and concurrency are configurable with `BLOCKS_INDEXER_BATCH_SIZE` and `BLOCKS_INDEXER_HISTORICAL_CONCURRENCY`).
    2.  Fetches logs and block bodies for each batch, tracking progress, remaining blocks, and ETA during catch-up.
    3.  Extracts Four.Meme trades and Pancake swap events.
    4.  Optionally archives raw blocks (RLP encoded) and log batches to dedicated RabbitMQ queues when started with `-archive`.
-   **Output:** Publishes `trade.Trade` messages to `trades`, `trade.Swap` messages to `pool-swaps`, and optionally raw data to `archive-*` queues.

### `pancake-relay`
-   **Input:** Consumes `trade.Swap` messages from the `pool-swaps` queue.
-   **Logic:** 
    1.  Fetches token pair information for the pool address, caching the result in Redis.
    2.  Filters for swaps where one of the tokens is WBNB.
    3.  Transforms the `Swap` data into the canonical `trade.Trade` format, determining the direction (BUY/SELL) and identifying the meme token.
    4.  Sets the `platform` field to `"Pancake"`.
-   **Output:** Publishes the final `trade.Trade` message to the `trades` queue.

### `swaps-consumer`
-   **Input:** Consumes `trade.Trade` messages from the `trades` queue.
-   **Logic:**
    1.  Buffers messages in memory.
    2.  Performs bulk inserts into a PostgreSQL database.
    3.  Handles conflicts (duplicate trades) gracefully.
    4.  Saves failed messages to a MongoDB collection for later analysis.
-   **Output:** Stores trade data in a PostgreSQL database.

### `archive-repeater`
-   **Input:** Reads historical logs from a MongoDB collection.
-   **Logic:**
    1.  Fetches logs in batches.
    2.  Parses the logs into `trade.Trade` or `trade.Swap` formats.
    3.  Publishes the parsed messages to the appropriate RabbitMQ queues.
    4.  Deletes the processed logs from MongoDB.
-   **Output:** Republishes historical data to the message queues.

### Infrastructure
-   **RabbitMQ:** Message broker with `pool-swaps` (raw swaps), `trades` (canonical trades), and optional archival queues `archive-blocks` / `archive-logs`. It is configured for persistence.
-   **Redis:** Used as a cache by the `pancake-relay` to store token metadata for pool addresses.
-   **MongoDB:** Receives archived payloads through the `archive-collector` service when archiving is enabled.

## Data Models

-   **`shared/trade/trade.go`**: Defines the canonical `Trade` struct. This is the standard format for all processed trades that end up in the `trades` queue.
-   **`shared/trade/swap.go`**: Defines the `Swap` struct, which represents a raw swap event from a DEX pool.
-   Archival payloads are simple JSON wrappers containing block numbers plus RLP-encoded bodies or raw log slices, enabling lossless replay.

## Configuration

All services are configured via a central `.env` file at the project root. This includes RPC endpoints, RabbitMQ credentials, and other environment-specific variables.

## How to Run

The entire stack can be built and run with a single command:

```shell
docker-compose up --build
```

To enable raw-data archiving, start the blocks indexer with the `-archive` flag (Docker Compose will pass it if you override `command` or `entrypoint`).
