package com.sqlrec.udf;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.linq4j.function.SemiStrict;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionContext;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

public class SqlRecScalarFunctionImpl extends ReflectiveFunctionBase
        implements ScalarFunction, ImplementableFunction {
    private final CallImplementor implementor;

    private SqlRecScalarFunctionImpl(Method method, CallImplementor implementor) {
        super(method);
        this.implementor = implementor;
    }

    public List<FunctionParameter> getParameters() {
        List<FunctionParameter> retParams = new ArrayList<>();
        Class<?>[] paramTypes = method.getParameterTypes();
        for (int i = 0; i < paramTypes.length; i++) {
            if (paramTypes[i].equals(DataContext.class)) {
                continue;
            }
            retParams.add(parameters.get(i));
        }
        return retParams;
    }

    public static List<ScalarFunction> createAll(Class<?> clazz, String methodName) {
        List<ScalarFunction> functions = new ArrayList<>();
        for (Method method : clazz.getMethods()) {
            if (!method.getName().equals(methodName) || method.isBridge()) {
                continue;
            }
            // Hive's execution entry point is not a SQL overload.
            if (method.getParameterCount() == 1
                    && method.getParameterTypes()[0] == DeferredObject[].class) {
                continue;
            }
            functions.add(create(method));
        }
        return functions;
    }

    public static ScalarFunction create(Method method) {
        if (!Modifier.isStatic(method.getModifiers())) {
            Class<?> clazz = method.getDeclaringClass();
            if (!classHasPublicZeroArgsConstructor(clazz)
                    && !classHasPublicFunctionContextConstructor(clazz)) {
                throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex();
            }
        }
        CallImplementor implementor = createImplementor(method);
        return new SqlRecScalarFunctionImpl(method, implementor);
    }

    static boolean classHasPublicZeroArgsConstructor(Class<?> clazz) {
        for (Constructor<?> constructor : clazz.getConstructors()) {
            if (constructor.getParameterTypes().length == 0
                    && Modifier.isPublic(constructor.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    static boolean classHasPublicFunctionContextConstructor(Class<?> clazz) {
        for (Constructor<?> constructor : clazz.getConstructors()) {
            if (constructor.getParameterTypes().length == 1
                    && constructor.getParameterTypes()[0] == FunctionContext.class
                    && Modifier.isPublic(constructor.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        if (List.class.isAssignableFrom(method.getReturnType())
                && method.getGenericReturnType() instanceof ParameterizedType
                && ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0] == Double.class) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createArrayType(typeFactory.createJavaType(Double.class), -1), true);
        }
        return typeFactory.createJavaType(method.getReturnType());
    }

    public boolean isDeterministic() {
        UDFType type = method.getDeclaringClass().getAnnotation(UDFType.class);
        return type == null || (type.deterministic() && !type.stateful());
    }

    @Override
    public CallImplementor getImplementor() {
        return implementor;
    }

    private static CallImplementor createImplementor(final Method method) {
        final NullPolicy nullPolicy = getNullPolicy(method);
        return RexImpTable.createImplementor(
                new SqlRecCallNotNullImplementor(method), nullPolicy, false);
    }

    private static NullPolicy getNullPolicy(Method m) {
        if (m.getAnnotation(Strict.class) != null) {
            return NullPolicy.STRICT;
        } else if (m.getAnnotation(SemiStrict.class) != null) {
            return NullPolicy.SEMI_STRICT;
        } else if (m.getDeclaringClass().getAnnotation(Strict.class) != null) {
            return NullPolicy.STRICT;
        } else if (m.getDeclaringClass().getAnnotation(SemiStrict.class) != null) {
            return NullPolicy.SEMI_STRICT;
        } else {
            return NullPolicy.NONE;
        }
    }
}
