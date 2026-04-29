package com.sqlrec.udf.scalar;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class L2NormFunction extends GenericUDF {
    private ListObjectInspector vectorOI;

    public List<Double> evaluate(Object vector) {
        if (vector == null) {
            return null;
        }

        if (!(vector instanceof List)) {
            throw new RuntimeException("L2NormFunction only support number list");
        }

        List<?> list = (List<?>) vector;
        double sum = 0;
        for (Object o : list) {
            if (o instanceof Number) {
                sum += Math.pow(((Number) o).doubleValue(), 2);
            } else {
                throw new RuntimeException("L2NormFunction only support number list");
            }
        }

        if (sum <= 0) {
            List<Double> result = new ArrayList<>();
            for (Object o : list) {
                result.add(((Number) o).doubleValue());
            }
            return result;
        }

        double norm = Math.sqrt(sum);
        List<Double> result = new ArrayList<>();
        for (Object o : list) {
            result.add(((Number) o).doubleValue() / norm);
        }
        return result;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("L2NormFunction requires exactly one argument.");
        }

        if (!(arguments[0] instanceof ListObjectInspector)) {
            throw new UDFArgumentTypeException(0, "The argument must be a list.");
        }

        this.vectorOI = (ListObjectInspector) arguments[0];

        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        return ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null || arguments[0].get() == null) {
            return null;
        }

        Object vector = arguments[0].get();
        List<?> vectorList = vectorOI.getList(vector);

        try {
            return evaluate(vectorList);
        } catch (RuntimeException e) {
            throw new HiveException(e.getMessage());
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "l2_norm(" + (children.length > 0 ? children[0] : "null") + ")";
    }
}
