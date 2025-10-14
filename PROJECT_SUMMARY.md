# Project Summary: bsc-memes-indexer

This document provides a high-level overview of the project architecture, services, and data flow.

## Architecture

This project follows a microservices architecture designed to capture cryptocurrency swap events from the Binance Smart Chain (BSC), process them, and standardize them into a single message queue.

The system is orchestrated using Docker Compose and consists of several Go applications that communicate via a RabbitMQ message broker. Redis is used for caching blockchain data to minimize RPC calls.

### Data Flow

1.  Two "producer" services listen for swap events on the BSC blockchain.
    *   `four-meme-producer`: Listens for specific events from a single, hardcoded contract.
    *   `pancake-producer`: Listens for generic `Swap` events from all contracts across the chain.
2.  The producers publish raw event data to two different RabbitMQ queues.
3.  The `pancake-relay` service consumes messages from the generic `pool-swaps` queue.
4.  The relay enriches these messages by fetching token metadata (with Redis caching), filters for pairs involving WBNB, and transforms the data into a standardized `Trade` format.
5.  The relay then publishes this standardized `Trade` message to the final `trades` queue.
6.  The `four-meme-producer` also publishes directly to the `trades` queue, as its data is already in the specific format required.

The final result is a single, clean stream of `Trade` messages in the `trades` queue, ready for consumption.

## Services

### `four-meme-producer`
-   **Source:** Listens to events from a hardcoded contract address (`0x5c95...`).
-   **Logic:** Transforms the specific event data into the standard `trade.Trade` format.
-   **Output:** Publishes `trade.Trade` messages to the `trades` queue.

### `pancake-producer`
-   **Source:** Listens for generic `Swap` events across all BSC contracts.
-   **Logic:** Parses the `Swap` event and publishes it without much transformation.
-   **Output:** Publishes `trade.Swap` messages to the `pool-swaps` queue.

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
-   **RabbitMQ:** Message broker with two main queues: `pool-swaps` (for raw data) and `trades` (for standardized data). It is configured for persistence.
-   **Redis:** Used as a cache by the `pancake-relay` to store token metadata for pool addresses.

## Data Models

-   **`shared/trade/trade.go`**: Defines the canonical `Trade` struct. This is the standard format for all processed trades that end up in the `trades` queue.
-   **`shared/trade/swap.go`**: Defines the `Swap` struct, which represents a raw swap event from a DEX pool.

## Configuration

All services are configured via a central `.env` file at the project root. This includes RPC endpoints, RabbitMQ credentials, and other environment-specific variables.

## How to Run

The entire stack can be built and run with a single command:

```shell
docker-compose up --build
```
