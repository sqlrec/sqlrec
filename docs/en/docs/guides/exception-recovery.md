# Timeouts, Fallbacks, and Error Recovery

An occasional remote-service, recall-source, or partition failure does not always mean that the whole recommendation request must fail. SQLRec provides several recovery patterns; choose one according to the business meaning of the failure:

| Goal | Recommended option |
| --- | --- |
| Use a fallback when the primary path fails | `IF TIMEIN ... ELSE ...` |
| Keep merging when one recall source fails | `IGNORE_UNION_EXCEPTION` |
| Continue other rows when one external Join lookup fails | `IGNORE_JOIN_QUERY_EXCEPTION` |
| Keep successful partitions when some partitions fail | `IGNORE_PARTITION_EXCEPTION` |
| Fail the request on any error | Keep the corresponding ignore option disabled |

## Fallback for timeout or exception: `IF TIMEIN`

`IF TIMEIN` handles timeouts and failures in THEN, including `Error` and `InterruptedException`.

```sql
IF TIMEIN (SELECT 100 FROM config_table) THEN (
    CACHE TABLE result_table AS CALL remote_rank_service(candidate_items)
) ELSE (
    CACHE TABLE result_table AS SELECT * FROM local_rank_fallback
);
```

Here `100` is milliseconds:

- a successful THEN result is used when it finishes within 100ms;
- a timeout cancels THEN and runs ELSE;
- a THEN exception or error, including interruption, runs ELSE;
- an ancestor cancellation does not run ELSE and terminates the request.

Cache data is fully computed before publication. Publication and context cancellation use the same schema monitor: after cancellation completes, a late THEN cannot overwrite the cache written by ELSE. SQL callees and partitions use independent temporary schemas and do not share publication locks.

An outer `NODE_EXEC_TIMEOUT` covers the entire ordinary `IF` or `IF TIMEIN`, so condition, THEN, and ELSE execution do not reset the node budget. Once the node timeout cancels the IF, it does not run ELSE.

The timeout can come from a variable:

```sql
IF TIMEIN (
    SELECT CAST(`get_or_default`('rank_timeout_ms', '80') AS BIGINT)
) THEN (
    CACHE TABLE result_table AS CALL rank_service(items)
) ELSE (
    CACHE TABLE result_table AS SELECT * FROM items
);
```

`TIMEIN` is the time budget available to THEN. A non-positive budget skips THEN and executes ELSE directly. Set the budget to `0` to select the fallback explicitly:

```sql
IF TIMEIN (SELECT 0) THEN (
    CACHE TABLE result_table AS CALL rank_service(items)
) ELSE (
    CACHE TABLE result_table AS SELECT * FROM items
);
```

The example does not call `rank_service`; it uses the `items` result from ELSE immediately. Negative budgets follow the same rule, so a computed remaining budget that has expired also selects ELSE.

When migrating configurations, replace any `0` or negative TIMEIN values that previously meant “no timeout” with a sufficiently large positive budget for the application. This remains a finite budget and is subject to outer node timeouts and request cancellation. `NODE_EXEC_TIMEOUT <= 0` continues to disable the node timeout.

Direct fallback records `sqlrec_if_cache_direct_fallback_total`, an actual waiting timeout records `sqlrec_if_cache_timeout_total`, and ordinary THEN failures record `sqlrec_if_cache_exception_fallback_total`. These counters record branch selections; failures in ELSE still propagate. An already cancelled request does not select ELSE or increment a fallback counter.

When both branches return data, their schemas must be compatible:

```sql
IF TIMEIN (SELECT 50) THEN (
    RETURN SELECT * FROM service_result
) ELSE (
    RETURN SELECT * FROM cached_result
);
-- End the function definition
RETURN;
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

For an eligible cache branch that is eventually consumed by a UNION, a failure or node timeout is treated as an empty branch and the other inputs continue. This includes `Error`, `InterruptedException`, and wrapped failures. Recovery clears the current thread's handled interrupt flag so later nodes can continue. This is suitable for recall and optional feature sources.

The failed node's child context is cancelled while the parent context can continue. Late computation results from a timed-out cache node are discarded and cannot overwrite the recovered empty table.

Disable it when the branch is mandatory:

```sql
SET 'IGNORE_UNION_EXCEPTION' = 'false';
```

It does not apply when the failed result is returned directly, also feeds a non-UNION path, or the request is cancelled.

Restrictions apply to individual nodes; an IF does not disable recovery for the entire SQL function:

- `IF` / `IF TIMEIN` is neither a UNION merge boundary nor eligible for automatic empty-table recovery, even if its result later feeds a UNION.
- When multiple nodes write the same table, none of those writers can recover automatically. This avoids guessing which table version a conditional write preserves or replaces.

A callee's restrictions do not propagate to its caller. For both static and dynamic `CALL`, recovery depends on how the caller uses the result. An eligible UNION branch may treat a failed call as empty, even when the error originated from an IF or repeated-write node inside the callee.

Within the current function, protected nodes and their upstream producers cannot recover automatically. This does not disable eligible UNION recovery inside a callee. A successfully returned result can feed an outer IF condition or other operation; if the callee throws an error, the caller decides how to handle it based on how it uses the call result. Unrelated UNION branches remain eligible. The ELSE fallback provided by `IF TIMEIN` remains available.

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

For a primary-key KV Join, a failed bulk lookup falls back to unexpired cached rows for the requested keys, without retrying individual backend lookups. Missing keys, or a disabled cache, produce no match; LEFT JOIN retains the left row with NULLs on the right. A non-primary-key KV Join skips failed keys and continues other lookups. Vector Join produces no match for the failed left row and continues with other rows.

For strict behavior:

```sql
SET 'IGNORE_JOIN_QUERY_EXCEPTION' = 'false';
```

This handles external lookup failures. Missing tables, invalid schemas, and query-planning errors still fail the query. KV Join cancellation, interruption and execution timeout exceptions propagate; cache fallback applies only to primary-key joins.

## Keep successful partitions: `IGNORE_PARTITION_EXCEPTION`

By default, any failed partition immediately ends the wait for the partitioned call and cancels its child context; running tasks exit cooperatively:

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

Each partition has its own child context; a failure cancels only that partition and its background descendants. Interrupting the waiting thread preserves the interrupt flag, cancels the partitioned call, and raises an error in both strict and partial-result modes.

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

The following normally indicate that the request cannot continue and should not be degraded: failed `ASSERT`, missing tables/functions/fields, incompatible schemas, a SQL function that never executes `RETURN`, and ancestor cancellation.

Request-context cancellation always terminates execution and prevents UNION recovery or an `IF TIMEIN` ELSE fallback. Thread interruption alone does not mean that the request context has been cancelled.

During parallel execution, a node failure that is not recovered immediately ends the wait and propagates the exception. Other nodes continue to use context cancellation checks; running tasks exit cooperatively.

Recovery changes the failure scope, not result ordering. Add an explicit `ORDER BY` whenever stable ordering is required.
