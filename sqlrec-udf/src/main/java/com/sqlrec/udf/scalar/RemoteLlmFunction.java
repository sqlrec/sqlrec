package com.sqlrec.udf.scalar;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/** Hive bridge shared by the five-string remote scalar functions. */
public abstract class RemoteLlmFunction extends GenericUDF {
    private final PrimitiveObjectInspector[] inspectors = new PrimitiveObjectInspector[5];

    protected abstract boolean returnsVector();

    protected abstract Object invoke(String endpoint, String apiKey, String model, String input, String options);

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 5) {
            throw new UDFArgumentLengthException("Remote LLM functions require exactly five string arguments");
        }
        for (int i = 0; i < arguments.length; i++) {
            if (!(arguments[i] instanceof PrimitiveObjectInspector)) {
                throw new UDFArgumentTypeException(i, "Argument must be a string");
            }
            PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) arguments[i];
            switch (inspector.getPrimitiveCategory()) {
                case STRING:
                case CHAR:
                case VARCHAR:
                case VOID:
                    inspectors[i] = inspector;
                    break;
                default:
                    throw new UDFArgumentTypeException(i, "Argument must be a string");
            }
        }
        return returnsVector()
                ? ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)
                : PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String input = string(arguments[3], 3);
        if (input == null) {
            return null;
        }
        try {
            return invoke(string(arguments[0], 0), string(arguments[1], 1),
                    string(arguments[2], 2), input, string(arguments[4], 4));
        } catch (RuntimeException e) {
            throw new HiveException(e.getMessage(), e);
        }
    }

    private String string(DeferredObject value, int index) throws HiveException {
        Object object = value == null ? null : value.get();
        return object == null ? null : PrimitiveObjectInspectorUtils.getString(object, inspectors[index]);
    }

    @Override
    public String getDisplayString(String[] children) {
        // Hive explain/logging must not interpolate the api_key argument here.
        return getClass().getSimpleName() + "(<redacted>)";
    }
}
