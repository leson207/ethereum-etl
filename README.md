# Introduction
The main goal of this project is to create an Ethereum ETL codebase that anyone can easily clone and customize. It is inspired by the original ethereum-etl project (https://github.com/blockchain-etl/ethereum-etl).

Key Improvements Over the Original Ethereum-ETL:
- Modern Python: Built with Python 3.13.7, removing the legacy Python 2 constraints.
- Better Memory Efficiency: Benefits from improvements in modern Python versions.
- Faster Requests: Utilizes asynchronous programming instead of threads.
- Batch Processing: Supports batch requests instead of single requests for higher throughput.
- Optimized Querying: Fetches receipts by block instead of individual transactions.
- Improved Design: Cleaner architecture for easier maintenance and extension.
- Richer information: More data from many source (binance, etherscan, 4byte)

Workflow Overview:
- Data Collection: Fetch online data from multiple sources.
- Extraction & Enrichment: Process and enhance the raw data.
- Exporting: Send the processed data to the designated exporter.

Customization Points:
- Pick URI Strategy
- Schema
- Exporter
- Cache service
- Dex: currently support uniswapv2 and uniswapv3
- Clients: Add more client due to use case.
- RPC retry

# How to use
## Enrironment
```
# Copy template and configure environment
cp .env.template .env # put your api key here

# Setup python environment
uv venv
uv sync
source .venv/bin/activate

docker compose -f ./system/docker-compose up -d
```

## Features
Historical Mode: Extract data from an RPC block range, process entities, and send them to the exporter.
```
# Here how a full command looklike,  reduce range, remove contract, abi and exporter for fast try
python -m src.clis.historical --start-block 23000000 --end-block 23005000 \
    --process-batch-size 1000 --request-batch-size 30 \
    --entity-types raw_block,block,transaction,withdrawal,raw_receipt,receipt,log,transfer,event,account,contract,abi,pool,token,raw_trace,trace \
    --exporter-types sqlite
```

Realtime Mode: Subscribe to new events via an RPC WebSocket. As soon as a new block is detected, extract entities and forward them to the exporter.

# Client
## Design Principles
- Each online data source is represented by a dedicated client class.
- Each client exposes methods that behave like function calls for easy use.
- This design makes it straightforward to swap implementations, debug issues, and extend functionality.
- Every client has a built-in rate limiter (throttler) with a fixed request cap. When creating a new client, you must research and manually configure the appropriate limit.
- Clients also include a retry mechanism to handle common failures (primarily rate-limiting issues).

    - ⚠️ Even if the throttler is set properly, rate-limiting errors may still occur (e.g., Etherscan officially allows 5 requests/s, but limits are sometimes enforced more strictly in practice).

## Error Handling
Clients must handle two types of errors:
- Network Errors – e.g., connection drops, timeouts.
- Response Errors – server returns JSON containing an error, requiring a retry.

Retry attempts can be set to infinite since most errors are due to rate limiting. Default is currently 5 retries.

## Existing Clients
### RpcClient
- Batch size: default request batchsize is well tested and good enough but you can try it your self.
- Rate Limit: 50 requests/s (not official, chosen to balance speed without excessive failures).
- URI Strategy: Uses round-robin selection with free URIs first and limited URIs later. This can be swapped for other strategies (e.g., random) to improve throughput.

### BinanceClient
- Used to fetch ETH prices at specific block timestamps.
- Rate Limit: 50 requests/s (not official, not stress-tested, but stable in practice).
### 4byteClient
- Purpose: Decode event hashes.
- Status: Currently unused.
### EtherscanClient
- Fetches contract information, including contract metadata and ABI.
- Official Free Rate Limit: 5 requests/s.
Note: Data from this client is not well-tested at scale due to slow performance.

# Exporter
Currently supported exporters:
- SQLite → Lightweight storage, simple to set up.
- NATS → Fast and lightweight messaging system. (Kafka’s benefits are not as clear here, since message density is relatively low.)
- ClickHouse → High-performance analytics database (currently limited to 'event' table and use all type as string).
    ⚠️ If ClickHouse does not have a proper schema defined, it will reject messages coming from NATS.

# Cache
- Default: Uses SQLite as the cache layer. 
- Recommendation: Switch to Dragonfly (not redis) for better performance and scalability.
Note: Some cached data (e.g., WETH price) is primarily for testing when repeatedly running queries over a block range.

# Future Work
- Centralize connection: create connection manager for databases, message queues, objec tsore, ...
- Calculate token price of a pool: Contruct a token graph for this. Algorithm for this task - shortest path - is supportted by any graph database but recommend memgraph for speed and rich set of algorithm.

Token Price Calculation for Pools: Construct a token graph to calculate pool token prices. Algorithm for this task - shortest path - is supportted by any graph database but recommend memgraph for speed and rich set of algorithm.

# Disclaimer
⚠️ This project is under active development and may contain bugs.

Use at your own risk. Contributions, feedback, and improvements are always welcome.