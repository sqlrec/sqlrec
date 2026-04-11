# Benchmark

This document introduces SQLRec performance testing methods and results.

## Test Environment

**Hardware Configuration**:
- CPU: AMD Ryzen 5600H
- Memory: 32GB DDR4

**Software Environment**:
- Operating System: Debian 12
- Kubernetes: Minikube
- SQLRec: Single instance deployment

## Test Data

Default test configuration is as follows:

| Configuration Item | Value |
|-------------------|-------|
| Number of Users | 100,000 |
| Number of Items | 100,000 |
| Vector Dimension | 8 dimensions |
| User Embedding | Fixed value |

## Recommendation Pipeline

The tested recommendation pipeline includes the following stages:

### Recall Stage

| Recall Strategy | Description | Recall Count |
|----------------|-------------|--------------|
| Global Hot Recall | Based on global item popularity ranking | 300 |
| User Interest Category Recall | Recall hot items based on user interest categories | 300 |
| ItemCF Recall | Recall based on item collaborative filtering | 300 |
| Vector Search Recall | Based on vector similarity search | 300 |

### Filtering Stage

| Filtering Strategy | Description |
|-------------------|-------------|
| Exposure Deduplication | Filter items already exposed to users |
| Category Diversification | Display at most N items per category |

## Test Scripts

### Initialize Test Environment

```bash
cd benchmark
bash init.sh
```

The `init.sh` script performs the following operations:

1. **Create Milvus Vector Collection**
   - Create `item_embedding` collection
   - Define vector dimension as 8
   - Create COSINE similarity index

2. **Create Data Tables**
   - User table (`user_table`)
   - Item table (`item_table`)
   - Global hot items table (`global_hot_item`)
   - User interest category table (`user_interest_category1`)
   - Category hot items table (`category1_hot_item`)
   - User recent clicks table (`user_recent_click_item`)
   - User exposure table (`user_exposure_item`)
   - ItemCF I2I table (`itemcf_i2i`)
   - Item vector table (`item_embedding`)
   - Recommendation log table (`rec_log_kafka`)

3. **Generate Simulated Data**
   - Use Python scripts to generate 100,000 users and 100,000 items data
   - Generate user behavior data and upload to HDFS

4. **Install Test Tools**
   - Install wrk HTTP benchmarking tool

### Execute Performance Test

```bash
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

`request.lua` is a custom request script for wrk:

```lua
-- Set random seed
math.randomseed(os.time())

function request()
    -- Generate random ID between 0-99999
    local random_id = math.random(0, 99999)
    
    -- Construct request body
    local request_body = string.format(
        '{"inputs":{"user_info":[{"id":%d}]},"params":{"recall_fun":"recall_fun"}}',
        random_id
    )
    
    -- Configure HTTP request
    wrk.method = "POST"
    wrk.headers["Content-Type"] = "application/json"
    wrk.body = request_body
    
    return wrk.format()
end
```

## Test Results

Test results on AMD Ryzen 5600H, 32GB DDR4 memory machine:

```
Running 30s test @ http://192.168.49.2:30301/api/v1/main_rec
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
| Latency Standard Deviation | 5.04ms |
| Max Latency | 48.96ms |
| Average QPS | 111.59 |
| Total Requests | 33,370 |
| Total QPS | 1111.47 |
| Throughput | 1.93MB/s |
