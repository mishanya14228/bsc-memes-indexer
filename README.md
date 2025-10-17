# Project info
скорми боту [PROJECT_SUMMARY.md](https://github.com/mishanya14228/bsc-memes-indexer/blob/main/PROJECT_SUMMARY.md) и [PROMPT_HISTORY.md](https://github.com/mishanya14228/bsc-memes-indexer/blob/main/PROMPT_HISTORY.md)

# Blocks Indexer
- `blocks-indexer` is now the primary producer for both Four.Meme trades and Pancake swap events.
- Use `docker compose up --build blocks-indexer` to start catching up from the last saved height in Redis.
- Progress logs include remaining blocks, percentage complete, and ETA while historical catch-up runs.
- To mirror raw blockchain data for recovery, run the indexer with `-archive` (e.g. `docker compose run --rm blocks-indexer ./server -archive`). When enabled, raw blocks/logs are pushed to `archive-*` queues and persisted by `archive-collector` in MongoDB.

# Archive Collector
- Subscribes to `archive-blocks` and `archive-logs`, writing each message into Mongo collections with matching names (`ARCHIVE_DB_NAME` defaults to `archives`).
- Start it alongside RabbitMQ/Mongo with `docker compose up --build archive-collector`.

# Fetching specific blocks
docker compose run --rm historical-fetcher ./server -from 63894599 -to 63894600
