package com.sqlrec.udf.scalar;

import com.sqlrec.common.utils.DataTransformUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

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

        return DataTransformUtils.l2NormalizeList((List<?>) vector);
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
