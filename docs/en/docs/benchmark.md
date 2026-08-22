# Benchmark

This document introduces SQLRec performance testing methods and results. The test is based on the [MovieLens-1M](https://grouplens.org/datasets/movielens/) dataset, and the corresponding scripts are located in the `benchmark/movielens/` directory.

## Test Environment

**Hardware Configuration**:
- CPU: AMD Ryzen 5600H
- Memory: 32GB DDR4

**Software Environment**:
- Operating System: Debian 12
- Kubernetes: Minikube
- SQLRec: Single instance deployment

## Test Data

The test uses the MovieLens-1M dataset. The default test configuration is as follows:

| Configuration Item | Value |
|-------------------|-------|
| Dataset | MovieLens-1M |
| Number of Users | 6040 |
| Number of Items | 3706 (movies) |
| Rating Records | ~1 million |
| Vector Dimension | 64 dimensions |
| User Embedding | Generated randomly via `random_vec` per request (when recall model service is not enabled) |

## Recommendation Pipeline

The tested recommendation pipeline is the `main_rec` function (defined in `benchmark/movielens/init_sqlrec_sql.sql`), which includes the following stages:

### Recall Stage

| Recall Strategy | Description | Recall Count |
|----------------|-------------|--------------|
| Global Hot Recall | Based on global item popularity ranking (`global_hot_item`) | 300 |
| User Interest Genre Recall | Recall hot items based on user interest genres (`user_interest_genre` + `genre_hot_item`) | 300 |
| ItemCF Recall | Based on user's recent clicked items (`user_recent_click_item` + `itemcf_i2i`) | 300 |
| Vector Search Recall | Based on the inner product similarity between user vectors and item vectors (Milvus) | 300 |

### Filtering and Re-ranking Stage

| Strategy | Description |
|----------|-------------|
| Exposure Deduplication | Filter items exposed to the user within the last 1 hour (`user_exposure_item`) |
| Ranking | Uses `rank_fun_simple` by default to join item metadata; can specify the wide_and_deep model-based ranking function via the API parameter `rank_fun` |
| Genre Diversification | `window_diversify`, window size 3, at most 1 item per genre within the window, finally returns 10 items |

### Other Stages

- Generate request metadata (`req_time`, `req_id`)
- Asynchronously write recommendation logs to Kafka (`rec_log_kafka`)
- Write recommendation results to the exposure table for subsequent deduplication

## Test Scripts

### Initialize Test Environment

```bash
cd benchmark/movielens
bash init.sh
```

The `init.sh` script performs the following operations:

1. **Deploy Kyuubi**: used for subsequent offline feature computation

2. **Install Test Tools**
   - Install wrk HTTP benchmarking tool

3. **Create Milvus Vector Collection**
   - Create `item_embedding` collection
   - Define vector dimension as 64
   - Create COSINE similarity index (AUTOINDEX)

4. **Download and Process Test Data**
   - Download the MovieLens-1M dataset
   - Convert to Parquet format (users, movies, ratings)
   - Upload to HDFS

5. **Create Data Tables**
   - User table (`user_table`), item table (`item_table`): Redis
   - Global hot items table (`global_hot_item`): Redis
   - User interest genre table (`user_interest_genre`): Redis
   - Genre hot items table (`genre_hot_item`): Redis
   - User recent clicks table (`user_recent_click_item`): Redis
   - User exposure table (`user_exposure_item`): Redis
   - ItemCF I2I table (`itemcf_i2i`): Redis
   - Item vector table (`item_embedding`): Milvus
   - Recommendation log table (`rec_log_kafka`): Kafka

6. **Compute Offline Features**: execute Spark SQL via Kyuubi to compute offline feature tables such as global hot items, user interest genres, genre hot items, and ItemCF I2I

7. **Train Models**: create and train the wide_and_deep ranking model (`rank_model`) and the DSSM two-tower recall model (`recall_model`), then export and deploy them as online services

8. **Load Feature Data**: load offline features into Redis, and generate item vectors by calling the recall model service via `batch_call_service` and write them into Milvus

9. **Register SQL Functions and API**: register SQL functions for recall, ranking, diversification, logging, etc., and create the `main_rec` API

10. **Test Recommendation**: invoke `main_rec` via beeline to verify the recommendation pipeline

### Execute Performance Test

```bash
cd benchmark/movielens
bash benchmark.sh
```

The `benchmark.sh` script performs the following operations:

1. **Warm-up Phase**
   - Single thread, single connection, run for 10 seconds
   - Warm up system cache

2. **Formal Testing**
   - Concurrency: 10
   - Duration: 30 seconds
   - Test URL: `/api/v1/main_rec`

### Test Request Script

`request.lua` is a custom request script for wrk. It generates a random user ID for each request and randomly prints some responses for verification:

```lua
-- Set random seed
math.randomseed(os.time())

function request()
    -- Generate random ID between 0-5000
    local random_id = math.random(0, 5000)

    -- Construct request body
    local request_body = string.format('{"data":{"user_info":[{"user_id":%d}]},"params":{"recall_fun":"recall_fun"}}', random_id)

    -- Configure HTTP request
    wrk.method = "POST"
    wrk.headers["Content-Type"] = "application/json"
    wrk.body = request_body

    return wrk.format()
end

-- Response handler to print response if the corresponding request was logged
function response(status, headers, body)
    current_request_log = (math.random(1, 100) == 1)
    if current_request_log then
        print("Response:")
        print("Status: " .. status)
        print("Body: " .. body)
    end
end
```

The `params` in the request body are set as execution context variables, e.g. `recall_fun` specifies the recall function name and `rank_fun` specifies the ranking function name.

## Test Results

Test results on AMD Ryzen 5600H, 32GB DDR4 memory machine:

```
Running 30s test @ http://192.168.49.2:30001/api/v1/main_rec
  10 threads and 10 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     9.23ms    5.04ms  48.96ms   90.50%
    Req/Sec   111.59     17.07   151.00     59.47%
  33370 requests in 30.02s, 57.91MB read
  Socket errors: connect 0, read 33369, write 0, timeout 0
Requests/sec:   1111.47
Transfer/sec:      1.93MB
```

**Performance Metrics**:

| Metric | Value |
|--------|-------|
| Average Latency | 9.23ms |
| Latency Std Dev | 5.04ms |
| Max Latency | 48.96ms |
| Average QPS | 111.59 |
| Total Requests | 33,370 |
| Total QPS | 1111.47 |
| Throughput | 1.93MB/s |
