package com.sqlrec.frontend.thrift;

import com.sqlrec.common.utils.DataTransformUtils;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.frontend.utils.ThriftUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.apache.hive.service.rpc.thrift.TRowSet;

import java.util.List;

/** Local execution result and its fetch cursor. */
public class SqlOperation {
    private final SqlProcessResult coreResult;
    private final String queryId;
    private final boolean metadataOperation;
    private String msg;
    private Exception exception;
    private List<Object[]> resultRows;
    private int fetchOffset;

    public record ResultPage(TRowSet rows, boolean hasMoreRows) {}

    public SqlOperation(SqlProcessResult result, String queryId) {
        this(result, queryId, false);
    }

    private SqlOperation(SqlProcessResult result, String queryId, boolean metadataOperation) {
        this.coreResult = result;
        this.queryId = queryId;
        this.metadataOperation = metadataOperation;
    }

    public static SqlOperation metadata(SqlProcessResult result, String queryId) {
        return new SqlOperation(result, queryId, true);
    }

    /** Ordinary SQL consumes all rows once; JDBC metadata supports bounded, rewindable pages. */
    public synchronized ResultPage fetch(TFetchOrientation orientation, long maxRows) {
        return metadataOperation ? fetchMetadataPage(orientation, maxRows) : fetchSqlResult();
    }

    private ResultPage fetchSqlResult() {
        Enumerable<Object[]> rows = coreResult.getFields() == null
                ? DataTransformUtils.getMsgEnumerable("no output") : coreResult.getEnumerable();
        ResultPage page = resultPage(rows, 0, false);
        coreResult.setEnumerable(null);
        return page;
    }

    private ResultPage fetchMetadataPage(TFetchOrientation orientation, long maxRows) {
        if (orientation != TFetchOrientation.FETCH_NEXT && orientation != TFetchOrientation.FETCH_FIRST) {
            throw new UnsupportedOperationException("Only FETCH_NEXT and FETCH_FIRST are supported locally");
        }
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be positive");
        }
        if (resultRows == null) {
            Enumerable<Object[]> rows = coreResult.getEnumerable();
            resultRows = rows == null ? List.of() : rows.toList();
        }
        int start = orientation == TFetchOrientation.FETCH_FIRST ? 0 : fetchOffset;
        int end = start + (int) Math.min(maxRows, resultRows.size() - start);
        ResultPage page = resultPage(Linq4j.asEnumerable(resultRows.subList(start, end)), start,
                end < resultRows.size());
        fetchOffset = end;
        return page;
    }

    private ResultPage resultPage(Enumerable<Object[]> values, long offset, boolean hasMoreRows) {
        TRowSet rows = ThriftUtils.convertObjectArrayToTRowSet(values, getFields());
        rows.setStartRowOffset(offset);
        return new ResultPage(rows, hasMoreRows);
    }

    public List<RelDataTypeField> getFields() {
        return coreResult.getFields() == null ? DataTypeUtils.getStringTypeField("sys_warn") : coreResult.getFields();
    }

    public synchronized TOperationState getState() {
        if (exception != null) {
            return TOperationState.ERROR_STATE;
        }
        try {
            return coreResult.isCompleted() ? TOperationState.FINISHED_STATE : TOperationState.RUNNING_STATE;
        } catch (Exception e) {
            fail(e);
            return TOperationState.ERROR_STATE;
        }
    }

    public String getQueryId() {
        return queryId;
    }

    public synchronized String getMsg() {
        return msg;
    }

    public synchronized Exception getException() {
        return exception;
    }

    public synchronized void fail(Exception failure) {
        exception = failure;
        msg = "exec error: " + ExceptionUtils.getStackTrace(failure);
    }
}
