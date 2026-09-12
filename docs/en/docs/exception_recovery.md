# Exception Handling and Recovery

An occasional remote-service, recall-source, or partition failure does not always mean that the whole recommendation request must fail. SQLRec provides several recovery patterns; choose one according to the business meaning of the failure:

| Goal | Recommended option |
| --- | --- |
| Use a fallback when the primary path fails | `IF TIMEIN ... ELSE ...` |
| Keep merging when one recall source fails | `IGNORE_UNION_EXCEPTION` |
| Continue other rows when one external Join lookup fails | `IGNORE_JOIN_QUERY_EXCEPTION` |
| Keep successful partitions when some partitions fail | `IGNORE_PARTITION_EXCEPTION` |
| Fail the request on any error | Keep the corresponding ignore option disabled |

## Fallback for timeout or exception: `IF TIMEIN`

`IF TIMEIN` handles both timeout and ordinary exceptions in THEN. It is not limited to timeout handling.

```sql
IF TIMEIN (SELECT 100 FROM config_table) THEN (
    CACHE TABLE result AS CALL remote_rank_service(candidate_items)
) ELSE (
    CACHE TABLE result AS SELECT * FROM local_rank_fallback
);
```

Here `100` is milliseconds:

- a successful THEN result is used when it finishes within 100ms;
- a timeout cancels THEN and runs ELSE;
- an ordinary THEN exception runs ELSE;
- an ancestor cancellation does not run ELSE and terminates the request.

The timeout can come from a variable:

```sql
IF TIMEIN (
    SELECT CAST(get_or_default('rank_timeout_ms', '80') AS BIGINT)
) THEN (
    CACHE TABLE result AS CALL rank_service(items)
) ELSE (
    CACHE TABLE result AS SELECT * FROM items
);
```

A non-positive timeout means that no timeout is configured and THEN may run indefinitely. Ordinary exceptions from THEN still fall back to ELSE:

```sql
IF TIMEIN (SELECT 0) THEN (
    CACHE TABLE result AS CALL rank_service(items)
) ELSE (
    CACHE TABLE result AS SELECT * FROM items
);
```

The example does not impose a timeout because the value is `0`, but an exception from `rank_service` still selects ELSE. The THEN result is committed only after successful completion.

When both branches return data, their schemas must be compatible:

```sql
IF TIMEIN (SELECT 50) THEN (
    RETURN SELECT * FROM service_result
) ELSE (
    RETURN SELECT * FROM cached_result
);
```

Only a successfully completed THEN commits its return value. A timeout or exception cannot commit a partial THEN result. Ordinary `IF` does not automatically fall back after a THEN exception; use `IF TIMEIN` for that behavior.

## Continue merging recall sources: `IGNORE_UNION_EXCEPTION`

This option is useful when one recall source may be unavailable but other sources can still produce a valid response:

```sql
SET 'IGNORE_UNION_EXCEPTION' = 'true';

CACHE TABLE recall_a AS CALL itemcf_recall(user_profile);
CACHE TABLE recall_b AS CALL hot_recall(user_profile);
CACHE TABLE all_recall AS
    SELECT * FROM recall_a
    UNION ALL
    SELECT * FROM recall_b;
```

For an eligible cache branch that is eventually consumed by a UNION, an ordinary exception or node timeout is treated as an empty branch and the other inputs continue. This is suitable for recall and optional feature sources.

Disable it when the branch is mandatory:

```sql
SET 'IGNORE_UNION_EXCEPTION' = 'false';
```

It does not apply when the failed result is returned directly, also feeds a non-UNION path, the request is cancelled, or the function is selected dynamically with `GET()` and cannot be classified at compile time.

## Continue after an external Join lookup failure: `IGNORE_JOIN_QUERY_EXCEPTION`

Use this option when one KV or vector lookup should not fail the complete query:

```sql
SET 'IGNORE_JOIN_QUERY_EXCEPTION' = 'true';

CACHE TABLE enriched AS
SELECT u.user_id, i.item_id, i.category
FROM users u
LEFT JOIN item_features i
  ON u.item_id = i.item_id;
```

KV Join skips the failed key; a LEFT JOIN keeps the left row and pads the right side with NULL. Vector Join produces no match for the failed left row and continues with other rows.

For strict behavior:

```sql
SET 'IGNORE_JOIN_QUERY_EXCEPTION' = 'false';
```

This only handles an individual external lookup failure. Missing tables, invalid schemas, and query-planning errors still fail the query.

## Keep successful partitions: `IGNORE_PARTITION_EXCEPTION`

By default, any failed partition fails the complete partitioned call:

```sql
CACHE TABLE ranked AS
CALL rank_partition(items)
PARTITION BY items SIZE 100;
```

When partial results are acceptable, enable the option:

```sql
SET 'IGNORE_PARTITION_EXCEPTION' = 'true';

CACHE TABLE ranked AS
CALL rank_partition(items)
PARTITION BY items SIZE get_or_default('partition_size', '100');
```

With the option enabled, successful partitions are merged and failed partition results are discarded. At least one successful partition returns partial results; if every partition fails, the call still raises an error. Cancellation, timeout, and serious errors are not treated as ordinary partition failures.

Disable it to restore strict behavior:

```sql
SET 'IGNORE_PARTITION_EXCEPTION' = 'false';
```

`SIZE` supports both legacy literals and runtime variables:

```sql
PARTITION BY items SIZE 100
PARTITION BY items SIZE get_or_default('partition_size', '100')
PARTITION BY items SIZE get('partition_size')
```

With a positive size, an empty input creates no partitions and returns an empty result. `ASYNC` is fire-and-forget, so the caller cannot synchronously observe partition failure; use synchronous `CALL` when partial versus total failure matters.

## Configuration precedence

For options read from the execution context, precedence is:

```text
current execution parameter > environment variable > code default
```

For example:

```sql
SET 'IGNORE_PARTITION_EXCEPTION' = 'true';
```

This affects the current execution context. If the execution does not set the option, SQLRec checks the environment variable with the same name, then uses the declared default.

| Option | Default | Use |
| --- | --- | --- |
| `IGNORE_UNION_EXCEPTION` | `true` | Degrade eligible cache branches before UNION |
| `IGNORE_JOIN_QUERY_EXCEPTION` | `true` | Ignore an individual KV/Vector Join lookup failure |
| `IGNORE_PARTITION_EXCEPTION` | `false` | Keep successful partition results |
| `NODE_EXEC_TIMEOUT` | `0` | Timeout for timeout-capable nodes, in milliseconds |

## Failures that should remain terminal

The following normally indicate that the request cannot continue and should not be degraded: failed `ASSERT`, missing tables/functions/fields, incompatible schemas, a SQL function that never executes `RETURN`, ancestor cancellation, thread interruption, and serious errors.

Recovery changes the failure scope, not result ordering. Add an explicit `ORDER BY` whenever stable ordering is required.
