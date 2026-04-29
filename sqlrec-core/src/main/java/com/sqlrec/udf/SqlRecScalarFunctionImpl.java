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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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

    public static @Nullable ScalarFunction create(Class<?> clazz, String methodName) {
        final Method method = findMethod(clazz, methodName);
        if (method == null) {
            return null;
        }
        return create(method);
    }

    static @Nullable Method findMethod(Class<?> clazz, String name) {
        Method preferredMethod = null;
        Method otherMethod = null;
        
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals(name) && !method.isBridge()) {
                if (isPreferParams(method)) {
                    preferredMethod = method;
                } else if (otherMethod == null) {
                    otherMethod = method;
                }
            }
        }
        
        return preferredMethod != null ? preferredMethod : otherMethod;
    }

    private static boolean isPreferParams(Method method) {
        for (Class<?> paramType : method.getParameterTypes()) {
            if (paramType != String.class && paramType != DataContext.class && paramType != Object.class) {
                return false;
            }
        }
        return true;
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
        return typeFactory.createJavaType(method.getReturnType());
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
