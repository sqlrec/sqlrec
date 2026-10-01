package com.sqlrec.frontend.thrift;

import com.sqlrec.executor.SqlProcessResult;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;

import java.util.List;

public class SqlOperation {
    private final SqlProcessResult coreResult;
    private final THandleIdentifier handleIdentifier;
    private final String queryId;
    private final boolean metadataOperation;
    private String msg;
    private Exception exception;
    private List<Object[]> resultRows;
    private int fetchOffset;

    public record ResultPage(Enumerable<Object[]> rows, long offset, boolean hasMoreRows) {}

    /** Materialize JDBC metadata once; ordinary SQL results retain their original fetch behavior. */
    public synchronized ResultPage fetch(TFetchOrientation orientation, long maxRows) {
        if (!metadataOperation) {
            throw new IllegalStateException("Pagination is only supported for JDBC metadata operations");
        }
        if (orientation != TFetchOrientation.FETCH_NEXT && orientation != TFetchOrientation.FETCH_FIRST) {
            throw new UnsupportedOperationException("Only FETCH_NEXT and FETCH_FIRST are supported locally");
        }
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be positive");
        }
        if (resultRows == null) {
            Enumerable<Object[]> enumerable = coreResult.getEnumerable();
            resultRows = enumerable == null ? List.of() : enumerable.toList();
        }
        if (orientation == TFetchOrientation.FETCH_FIRST) {
            fetchOffset = 0;
        }
        int start = fetchOffset;
        // Bound the addition before converting to int to avoid overflow for large JDBC fetch sizes.
        fetchOffset += (int) Math.min(maxRows, resultRows.size() - fetchOffset);
        return new ResultPage(Linq4j.asEnumerable(resultRows.subList(start, fetchOffset)),
                start, fetchOffset < resultRows.size());
    }

    public SqlOperation(SqlProcessResult coreResult, THandleIdentifier handleIdentifier, String queryId) {
        this(coreResult, handleIdentifier, queryId, false);
    }

    private SqlOperation(SqlProcessResult coreResult, THandleIdentifier handleIdentifier, String queryId,
                         boolean metadataOperation) {
        this.coreResult = coreResult;
        this.handleIdentifier = handleIdentifier;
        this.queryId = queryId;
        this.metadataOperation = metadataOperation;
    }

    public static SqlOperation metadata(SqlProcessResult result, THandleIdentifier handle, String queryId) {
        return new SqlOperation(result, handle, queryId, true);
    }

    public boolean isMetadataOperation() {
        return metadataOperation;
    }

    public Enumerable<Object[]> getEnumerable() {
        return coreResult.getEnumerable();
    }

    public void setEnumerable(Enumerable<Object[]> enumerable) {
        coreResult.setEnumerable(enumerable);
    }

    public List<RelDataTypeField> getFields() {
        return coreResult.getFields();
    }

    public boolean isCompleted() {
        return coreResult.isCompleted();
    }

    public THandleIdentifier getHandleIdentifier() {
        return handleIdentifier;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
}
