package com.sqlrec.udf.scalar;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

final class ArrayFunctionUtils {
    private ArrayFunctionUtils() {
    }

    static boolean contains(List<?> values, Object target) {
        for (Object value : values) {
            if (Objects.equals(value, target)) {
                return true;
            }
            if (value instanceof Number && target instanceof Number) {
                try {
                    if (new BigDecimal(value.toString()).compareTo(new BigDecimal(target.toString())) == 0) {
                        return true;
                    }
                } catch (NumberFormatException ignored) {
                    // NaN and infinities retain their Java equality semantics.
                }
            }
        }
        return false;
    }
}
