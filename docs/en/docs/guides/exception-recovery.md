# Timeouts, Fallbacks, and Error Recovery

Choose a recovery strategy according to whether the application accepts fallback or partial results:

| Goal | Option | Default |
| --- | --- | --- |
| Use a fallback when the primary path fails | `IF TIMEIN ... ELSE ...` | Explicit SQL |
| Continue merging when a recall source fails | `IGNORE_UNION_EXCEPTION` | `true` |
| Continue after an external Join lookup failure | `IGNORE_JOIN_QUERY_EXCEPTION` | `true` |
| Keep successful partitions | `IGNORE_PARTITION_EXCEPTION` | `false` |

Ignore options affect result completeness. Disable the corresponding option for required data sources so failures stop the request.

## Fallback for Timeout or Exception: `IF TIMEIN`

```sql
IF TIMEIN (SELECT 100) THEN (
  CACHE TABLE result_table AS CALL online_rank(items)
) ELSE (
  CACHE TABLE result_table AS SELECT * FROM items
);
```

`100` is the THEN time budget in milliseconds:

- A successful result within the budget uses THEN.
- A timeout or execution failure uses ELSE, including errors and interruptions during execution.
- A non-positive budget skips THEN and uses ELSE directly.
- Request cancellation or an outer node timeout prevents ELSE. Failures in ELSE still propagate.

Both branches must write the same cached table with compatible schemas, or both must use `RETURN`. A late or failed THEN result cannot overwrite the fallback. See [IF syntax](../reference/sql.md#if).

Read a dynamic budget from request variables:

```sql
IF TIMEIN (SELECT CAST(`get`('rank_timeout_ms', '80') AS BIGINT)) THEN (
  CACHE TABLE result_table AS CALL online_rank(items)
) ELSE (
  CACHE TABLE result_table AS SELECT * FROM items
);
```

Ordinary `IF` selects a branch; it does not run ELSE after a THEN failure. `TIMEIN <= 0` selects the fallback immediately, whereas `NODE_EXEC_TIMEOUT <= 0` disables the node timeout.

## Continue Merging Recall Sources: `IGNORE_UNION_EXCEPTION`

```sql
SET 'IGNORE_UNION_EXCEPTION' = 'true';

CACHE TABLE recall_a AS CALL itemcf_recall(user_info);
CACHE TABLE recall_b AS CALL hot_recall(user_info);
CACHE TABLE all_recall AS
SELECT * FROM recall_a
UNION ALL
SELECT * FROM recall_b;
```

A failed cache branch used only for UNION merging is treated as empty; other branches continue. Static `weighted_merge` calls also support recovery; dynamically selected `weighted_merge` calls do not.

Do not depend on automatic empty-table recovery for:

- Results returned directly or also used by non-merge operations.
- IF nodes in the current function, multiple writers of the same cached table, or their upstream dependencies.
- Cancelled requests.

A callee's failure is handled according to how its caller uses the result. Even an error from an IF inside a callee can be treated as empty when the whole call is used only by an eligible merge branch. Unrelated merge branches in the same function remain eligible.

Set `IGNORE_UNION_EXCEPTION=false` for required recall results, or use `IF TIMEIN` to define an explicit fallback.

## Continue After an External Join Lookup Failure: `IGNORE_JOIN_QUERY_EXCEPTION`

```sql
SET 'IGNORE_JOIN_QUERY_EXCEPTION' = 'true';

CACHE TABLE enriched AS
SELECT u.user_id, i.item_id, i.category
FROM users u
LEFT JOIN item_features i ON u.item_id = i.item_id;
```

| Lookup | Recovery |
| --- | --- |
| Primary-key KV Join | Use unexpired cached rows for the requested keys; missing keys produce no match, without individual backend retries |
| Non-primary-key KV Join | Skip the failed key and continue other lookups |
| Vector Join | Produce no match for the failed left row; continue other rows |

LEFT JOIN retains unmatched left rows with NULLs on the right; INNER JOIN omits the match. Set `IGNORE_JOIN_QUERY_EXCEPTION=false` for strict failure.

This option handles external lookup failures, not SQL, schema, or input errors. KV Join cancellation, interruption, and execution timeouts still propagate.

## Keep Successful Partitions: `IGNORE_PARTITION_EXCEPTION`

```sql
SET 'IGNORE_PARTITION_EXCEPTION' = 'true';

CACHE TABLE ranked AS
CALL rank_partition(items)
PARTITION BY items SIZE GET('partition_size', 100);
```

By default, any failed partition fails the call. With this option enabled, successful partitions are merged and failed ones discarded. At least one partition must succeed; if all fail, the call still fails. Cancellation, timeouts, and serious errors are not ignored as ordinary partition failures.

Empty input returns an empty result. Avoid `ASYNC` when the caller needs to observe completion. See [CALL](../reference/sql.md#call) for partition sizes and invocation rules.

## Configuration and Troubleshooting

Precedence is current execution parameter > same-named environment variable > default. Set execution parameters with SQL `SET` or API `params`.

`NODE_EXEC_TIMEOUT` sets the node budget in milliseconds; the default `0` disables it. One budget covers the whole IF, including condition, THEN, and ELSE; branches do not reset the timer.

Fix missing tables, functions, fields, or incompatible types detected during compilation. Runtime recovery depends on the branch; request cancellation always terminates execution. Add `ORDER BY` after merging when result order matters.

Use [Observability](../operations/observability.md#common-metrics) to inspect node failures, merge recovery, and TIMEIN outcomes and confirm that partial results are acceptable.
