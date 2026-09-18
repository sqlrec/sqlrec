package com.sqlrec.runtime;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.schema.CalciteSchemaFactory;
import com.sqlrec.sql.parser.SqlGetVariable;
import com.sqlrec.utils.ExecutorServiceUtils;
import com.sqlrec.utils.NodeUtils;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Executes one bindable once per partition and merges the successful results. */
final class PartitionExecutor {
    private static final Logger log = LoggerFactory.getLogger(PartitionExecutor.class);

    private final String tableName;
    private final SqlNode sizeNode;

    PartitionExecutor(String tableName, SqlNode sizeNode) {
        this.tableName = tableName;
        this.sizeNode = sizeNode;
    }

    Enumerable<Object[]> execute(
            CalciteSchema schema,
            ExecuteContext context,
            BindableInterface bindable
    ) {
        CacheTable table = SchemaUtils.getCacheTable(tableName, schema);
        List<RelDataTypeField> fields = table.getDataFields();
        List<Object[]> rows = new ArrayList<>();
        table.scan(null).forEach(rows::add);

        ExecuteContextImpl partitionContext = ((ExecuteContextImpl) context).clone();
        List<CompletableFuture<Enumerable<Object[]>>> futures = new ArrayList<>();
        for (List<Object[]> partition : split(rows, resolveSize(context))) {
            futures.add(CompletableFuture.supplyAsync(
                    () -> bindable.bind(createSchema(schema, partition, fields), partitionContext),
                    ExecutorServiceUtils.getExecutorService()));
        }
        return merge(context, partitionContext, futures);
    }

    private Enumerable<Object[]> merge(
            ExecuteContext context,
            ExecuteContextImpl partitionContext,
            List<CompletableFuture<Enumerable<Object[]>>> futures
    ) {
        boolean ignoreFailures = SqlRecConfigs.IGNORE_PARTITION_EXCEPTION
                .getValueWithEnvFallback(context.getVariables());
        List<Object[]> merged = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        int successes = 0;

        for (int i = 0; i < futures.size(); i++) {
            try {
                Enumerable<Object[]> result = futures.get(i).join();
                List<Object[]> partitionResult = new ArrayList<>();
                if (result != null) {
                    result.forEach(partitionResult::add);
                }
                merged.addAll(partitionResult);
                successes++;
            } catch (Exception exception) {
                Throwable failure = unwrap(exception);
                if (!ignoreFailures || context.isCancelled() || partitionContext.isCancelled()
                        || failure instanceof InterruptedException || failure instanceof Error) {
                    cancel(partitionContext, futures);
                    throw new RuntimeException("Partition execution failed", failure);
                }
                failures.add(failure);
                log.warn("[{}] partition {}/{} execution failed and its result is discarded: {}",
                        context.getLogId(), i + 1, futures.size(), failure.getMessage(), failure);
            }
        }

        if (successes == 0 && !failures.isEmpty()) {
            cancel(partitionContext, futures);
            RuntimeException allFailed = new RuntimeException(
                    "All " + failures.size() + " partition executions failed", failures.get(0));
            failures.stream().skip(1).forEach(allFailed::addSuppressed);
            throw allFailed;
        }
        return Linq4j.asEnumerable(merged);
    }

    private CalciteSchema createSchema(
            CalciteSchema source,
            List<Object[]> rows,
            List<RelDataTypeField> fields
    ) {
        CalciteSchema result = CalciteSchemaFactory.createCalciteSchema();
        for (String sourceTableName : source.getTableNames()) {
            CalciteSchema.TableEntry entry = source.getTable(sourceTableName, false);
            if (!(entry.getTable() instanceof CacheTable)) {
                continue;
            }
            if (NodeUtils.normalizeTableName(sourceTableName)
                    .equals(NodeUtils.normalizeTableName(tableName))) {
                result.add(sourceTableName,
                        new CacheTable(sourceTableName, Linq4j.asEnumerable(rows), fields));
            } else {
                result.add(sourceTableName, entry.getTable());
            }
        }
        return result;
    }

    private int resolveSize(ExecuteContext context) {
        if (sizeNode instanceof SqlLiteral literal) {
            return literal.intValue(false);
        }
        SqlGetVariable variable = (SqlGetVariable) sizeNode;
        String variableName = SchemaUtils.getValueOfStringLiteral(variable.getVariableName());
        String value = context.getVariable(variableName);
        if (value == null && variable.hasDefaultValue()) {
            value = SchemaUtils.getValueOfStringLiteral(
                    (SqlCharStringLiteral) variable.getDefaultValue());
        }
        if (value == null) {
            throw new RuntimeException("cant get partition size from variable: " + variableName);
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new RuntimeException(
                    "Invalid partition size from variable '" + variableName + "': '" + value + "'",
                    exception);
        }
    }

    private static List<List<Object[]>> split(List<Object[]> rows, int size) {
        if (size <= 0) {
            return Collections.singletonList(rows);
        }
        List<List<Object[]>> partitions = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += size) {
            partitions.add(rows.subList(i, Math.min(i + size, rows.size())));
        }
        return partitions;
    }

    private static Throwable unwrap(Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static void cancel(
            ExecuteContextImpl context,
            List<CompletableFuture<Enumerable<Object[]>>> futures
    ) {
        context.cancel();
        futures.forEach(future -> future.cancel(true));
    }
}
