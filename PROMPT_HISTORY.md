# Prompt History Summary

This document summarizes the development process and key decisions made during the construction of the `bsc-memes-indexer` project.

## 1. Initial Infrastructure Setup

- We began with an empty `docker-compose.yml`.
- The initial request was for Kafka, a Kafka GUI, and Redis with persistence.
- The plan was quickly pivoted to use **RabbitMQ** instead of Kafka, as the user was more familiar with it. The `docker-compose.yml` was updated accordingly.

## 2. Creating the `four-meme-producer` Service

- The first Go application was created to translate the logic from a TypeScript file (`logs-four-meme.ts`).
- The service's purpose is to listen for specific `buy` and `sell` events from a single, hardcoded contract address.
- We went through several iterations on the project structure:
    1. A single library file.
    2. A `main.go` file with a `lib/` directory.
    3. A single runnable file in the root.
    4. We finally landed on the current microservice architecture: **a separate directory for each runnable application** (`four-meme-producer/`, `pancake-producer/`, etc.).
- Configuration values (URLs, contract addresses) were externalized into a `.env` file and shared constant files in a `shared/` directory.
- The service was containerized with a `Dockerfile` and added to `docker-compose.yml`.

## 3. Creating the `pancake-producer` Service

- A second producer was created based on the logic from `typescript/logs.ts`.
- This service is more generic; it listens for standard `Swap` events from **all** contracts on the blockchain, creating a "firehose" of swap data.
- It publishes these raw swaps to a dedicated `pool-swaps` queue.
- During development, we encountered and fixed a bug where the `sender` and `to` addresses were incorrect. We diagnosed that this was due to not correctly parsing **`indexed` event topics**, and the logic was updated to read these addresses from the `log.Topics` array.
- This service was also containerized and added to `docker-compose.yml`.

## 4. Building the `pancake-relay` Service

- A third service was created to act as a processing pipeline.
- **Functionality:**
    1. Consumes raw `Swap` messages from the `pool-swaps` queue.
    2. Enriches the data by fetching `token0` and `token1` addresses for the pool contract.
    3. Caches this token data in **Redis** to minimize RPC calls.
    4. Filters messages, only processing swaps where one of the tokens is **WBNB**.
    5. **Transforms** the `Swap` data into the project's canonical `Trade` format, determining direction (BUY/SELL) and identifying the token amounts.
    6. Publishes the final, standardized `Trade` message to the `trades` queue.
- This service was containerized and added to `docker-compose.yml`.

## 5. System Hardening and Optimization

- **RPC Rate Limiting:** We implemented a **retry with exponential backoff** mechanism in the `pancake-relay` to gracefully handle `429 Too Many Requests` errors from the BSC RPC endpoint.
- **RabbitMQ Persistence:** We diagnosed and fixed an issue where RabbitMQ messages were lost on restart. The solution involved three parts:
    1. Ensuring queues were declared as `durable`.
    2. Ensuring messages were published as `persistent`.
    3. Adding a **persistent volume**, a fixed `hostname`, and a static `RABBITMQ_ERLANG_COOKIE` to the RabbitMQ service in `docker-compose.yml` to ensure its state was stable across restarts.
- **Docker Health Checks:** To solve startup race conditions, we added `healthcheck` configurations to the `rabbitmq` and `redis` services and updated the producers/relay to `depend_on` the services being healthy before starting.
- **RPC Optimization:** We identified that the producers were making an unnecessary RPC call per event to get a timestamp. We removed this call and replaced it with the local system time (`time.Now()`) to improve performance.

## 6. Documentation

- The process concluded with the creation of `PROJECT_SUMMARY.md` and this history file.

## 7. Implementing the `swaps-consumer` and `archive-repeater`

- **`swaps-consumer`:**
    - A new service was created to consume trades from the `trades` queue and insert them into a PostgreSQL database.
    - The implementation includes buffering, bulk inserts, and a fallback to individual inserts for error handling.
    - Failed messages are saved to a MongoDB collection.
    - The implementation was iterated upon to handle database constraints and to improve performance by using a single `INSERT` statement with multiple `VALUES` clauses.
- **`archive-repeater`:**
    - A new service was created to read historical logs from MongoDB, parse them, and republish them to the appropriate RabbitMQ queues.
    - The implementation includes batch processing, asynchronous deletion of processed logs, and concurrent processing to improve throughput.
    - The service was containerized and added to `docker-compose.yml`.
- **Docker and Localhost:**
    - Discussed how to access services running on the host's `localhost` from within a Docker container using `host.docker.internal`.
