# SparkMiner Unified Stats Endpoint

## Overview

GoPool provides a lightweight, read-only `/stats` endpoint designed specifically for SparkMiner devices. This endpoint returns essential pool statistics in a format optimized for ESP32 compatibility, avoiding the need for SparkMiner to make multiple HTTPS calls to external APIs.

## Endpoint

**Path:** `GET /stats`

**Port:** Status HTTP listener (default: `:8080`)

**Authentication:** None required (public endpoint)

**Cache:** Aggressive caching (15-second TTL) ensures fast responses

## Response Format

The endpoint returns JSON with the following optional fields (omitted when unavailable):

```json
{
  "btc_price_usd": 105420.50,
  "block_height": 881234,
  "network_hashrate": "789.5 EH/s",
  "network_difficulty": "95.67T",
  "fee_half_hour": 12,
  "fee_fastest": 25,
  "fee_hour": 8,
  "external_stats_age": 45,

  "workers": 3,
  "workersCount": 3,
  "pool_workers_count": 3,
  "pool_name": "GoPool",

  "pool_hashrate": "123.4 MH/s",
  "worker_hashrate": "52.3 KH/s",
  "address_best_diff": "0.0032",

  "difficulty_progress": 73.2,
  "difficulty_change": -1.4,
  "difficulty_retarget_blocks": 432
}
```

### Field Definitions

#### BTC and Blockchain Data

- **`btc_price_usd`**: Current BTC/USD price from CoinGecko (updated ~30 min)
- **`block_height`**: Bitcoin blockchain tip height
- **`network_hashrate`**: Estimated network hashrate (e.g., "789.5 EH/s")
- **`network_difficulty`**: Network difficulty (e.g., "95.67T")

#### Mempool Fees (sats/vB)

- **`fee_fastest`**: Fastest confirmation fee estimate (for next block)
- **`fee_half_hour`**: Fee for confirmation in ~30 minutes
- **`fee_hour`**: Fee for confirmation in ~1 hour
- **`external_stats_age`**: Age of oldest primary external stat cache in seconds

#### Worker/Pool Information

- **`workers`**, **`workersCount`**, **`pool_workers_count`**: Active miner count (all three fields provided for firmware compatibility)
- **`pool_name`**: Pool's brand name

#### Pool and Per-Worker Statistics

- **`pool_hashrate`**: Total pool hashrate (e.g., "123.4 MH/s")
- **`worker_hashrate`**: Individual worker hashrate (when wallet/worker params provided)
- **`address_best_diff`**: Best share difficulty for wallet (when wallet/worker params provided)

#### Difficulty Adjustment (VarDiff)

- **`difficulty_progress`**: Window completion % (0–100)
- **`difficulty_change`**: Expected % change at next adjustment
- **`difficulty_retarget_blocks`**: Estimated blocks until retarget

## Query Parameters

### Optional: Per-Worker Statistics

Provide both parameters to retrieve worker-specific stats:

```
GET /stats?wallet=<wallet_address>&worker=<worker_name>
```

- **`wallet`**: BTC wallet/pool account address
- **`worker`**: Worker name (optional; if omitted, global pool stats only)

**Response:** Includes `worker_hashrate` and `address_best_diff` for the specified worker.

## Cache Behavior

- **BTC Price**: 60–300 seconds (from CoinGecko, cached server-side)
- **Block Height**: 30–60 seconds (from Bitcoin RPC)
- **Network Fees**: 30–120 seconds (from mempool.space, cached server-side)
- **Network Difficulty/Hashrate**: 5–15 minutes (computed from block template)
- **Pool/Worker Stats**: 15 seconds (live from pool registry)

The `external_stats_age` field indicates how long ago external sources were last updated. SparkMiner should continue using cached values even if `external_stats_age` exceeds TTL thresholds.

## Content Type

```
Content-Type: application/json
```

## Example Usage

### cURL

```bash
# Global pool stats
curl http://localhost:8080/stats

# Per-wallet stats
curl 'http://localhost:8080/stats?wallet=bc1qabcd1234&worker=miner1'
```

### SparkMiner Configuration

Configure SparkMiner firmware to use the custom API URL:

```
customApiUrl=http://<gopool-host>:8080/stats?wallet=<wallet_address>&worker=<worker_name>
```

**Example:**
```
customApiUrl=http://192.168.1.100:8080/stats?wallet=bc1q9xypcx0nla6vva2zz8qy5qzdf8nly9jnqwqhq&worker=spark01
```

SparkMiner's `fetchFromCustomApi()` method will parse the JSON response without requiring firmware changes.

## Testing

### Run All Tests

```bash
go test -v ./... -run Spark
```

### Run Specific Tests

```bash
# Test JSON field names
go test -v . -run TestHandleSparkMinerStats_FieldNames

# Test content type
go test -v . -run TestHandleSparkMinerStats_ContentType

# Test cache hit behavior
go test -v . -run TestHandleSparkMinerStats_CacheHit

# Test difficulty formatting
go test -v . -run TestFormatDifficultyCompact
```

## Local Testing

**Start GoPool:**
```bash
./goPool -status :8080
```

**Test the endpoint:**
```bash
# Global stats (should return valid JSON)
curl -s http://localhost:8080/stats | jq .

# With wallet parameter
curl -s 'http://localhost:8080/stats?wallet=<wallet>' | jq .

# Check response headers
curl -i http://localhost:8080/stats
```

## Performance Considerations

1. **No Authentication**: Endpoint is publicly accessible (suitable for read-only stats).
2. **Aggressive Caching**: 15-second TTL prevents upstream overload.
3. **Minimal Payload**: JSON response is compact (~500 bytes typical) for ESP32 memory constraints.
4. **Fast Response**: Cached responses serve in <10ms; no blocking on external APIs.
5. **Graceful Degradation**: Returns partial data if any external sources are unavailable.

## Future Enhancements

- **Mempool.space Integration**: Currently returns `null` for fee fields; will fetch from public API or custom mempool instance.
- **Variance**: Per-worker historical stats (e.g., 24h average hashrate) could be added if needed.

## API Stability

The field names and response structure are designed for long-term stability to support SparkMiner firmware without breaking changes. Additions will use new field names only; existing fields will not be removed or renamed.
