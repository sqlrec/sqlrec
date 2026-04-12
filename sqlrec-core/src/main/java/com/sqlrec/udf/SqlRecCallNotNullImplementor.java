package com.sqlrec.udf;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class SqlRecCallNotNullImplementor extends ReflectiveCallNotNullImplementor {
    public SqlRecCallNotNullImplementor(Method method) {
        super(method);
    }

    @Override
    public Expression implement(RexToLixTranslator translator,
                                RexCall call, List<Expression> translatedOperands) {
        Class<?>[] paramTypes = method.getParameterTypes();
        List<Expression> operandList = new ArrayList<>();

        int curOriginOperandIndex = 0;
        for (Class<?> paramType : paramTypes) {
            if (paramType.equals(DataContext.class)) {
                operandList.add(Expressions.parameter(Object.class, "root"));
            } else {
                if (curOriginOperandIndex >= translatedOperands.size()) {
                    throw new IllegalArgumentException("Not enough operands for method " + method.getName());
                }
                operandList.add(translatedOperands.get(curOriginOperandIndex));
                curOriginOperandIndex++;
            }
        }

        if (operandList.size() > translatedOperands.size()) {
            return Expressions.call(method, operandList);
        }
        return super.implement(translator, call, operandList);
    }
}
