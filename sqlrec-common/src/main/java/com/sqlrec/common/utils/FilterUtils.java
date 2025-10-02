package com.sqlrec.common.utils;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.Collections;
import java.util.List;

public class FilterUtils {
    public static List<RexNode> getPrimaryKeyFilters(List<RexNode> filters, int primaryKeyIndex) {
        for (RexNode filter : filters) {
            if (filter.isA(SqlKind.EQUALS)) {
                RexCall call = (RexCall) filter;
                for (RexNode operand : call.getOperands()) {
                    if (operand instanceof RexInputRef) {
                        RexInputRef inputRef = (RexInputRef) operand;
                        if (inputRef.getIndex() == primaryKeyIndex) {
                            return Collections.singletonList(call);
                        }
                    }
                }
            } else if (filter.isA(SqlKind.AND)) {
                RexCall call = (RexCall) filter;
                List<RexNode> kvTableFilters = getPrimaryKeyFilters(call.getOperands(), primaryKeyIndex);
                if (kvTableFilters != null && !kvTableFilters.isEmpty()) {
                    return kvTableFilters;
                }
            }
        }
        return Collections.emptyList();
    }
}
