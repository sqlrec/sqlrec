package com.sqlrec.udf.scalar;

import com.sqlrec.common.utils.DataTransformUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class IpFunction extends GenericUDF {
    private ListObjectInspector emb1OI;
    private ListObjectInspector emb2OI;
    private PrimitiveObjectInspector emb1ElementOI;
    private PrimitiveObjectInspector emb2ElementOI;

    public Double evaluate(Object emb1, Object emb2) {
        if (emb1 == null || emb2 == null) {
            return null;
        }

        if (!(emb1 instanceof List) || !(emb2 instanceof List)) {
            throw new IllegalArgumentException("emb1 and emb2 must be list");
        }

        return DataTransformUtils.innerProduct((List<?>) emb1, (List<?>) emb2);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("IpFunction requires exactly two arguments.");
        }

        if (!(arguments[0] instanceof ListObjectInspector)) {
            throw new UDFArgumentTypeException(0, "The first argument must be a list.");
        }

        if (!(arguments[1] instanceof ListObjectInspector)) {
            throw new UDFArgumentTypeException(1, "The second argument must be a list.");
        }

        this.emb1OI = (ListObjectInspector) arguments[0];
        this.emb2OI = (ListObjectInspector) arguments[1];

        ObjectInspector emb1ElemOI = emb1OI.getListElementObjectInspector();
        ObjectInspector emb2ElemOI = emb2OI.getListElementObjectInspector();
        if (emb1ElemOI instanceof PrimitiveObjectInspector) {
            this.emb1ElementOI = (PrimitiveObjectInspector) emb1ElemOI;
        }
        if (emb2ElemOI instanceof PrimitiveObjectInspector) {
            this.emb2ElementOI = (PrimitiveObjectInspector) emb2ElemOI;
        }

        return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null || arguments[0].get() == null ||
                arguments[1] == null || arguments[1].get() == null) {
            return null;
        }

        Object emb1 = arguments[0].get();
        Object emb2 = arguments[1].get();

        List<Object> emb1List = toJavaList(emb1OI.getList(emb1), emb1ElementOI);
        List<Object> emb2List = toJavaList(emb2OI.getList(emb2), emb2ElementOI);

        try {
            return evaluate(emb1List, emb2List);
        } catch (IllegalArgumentException e) {
            throw new HiveException(e.getMessage());
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "ip(" + (children.length > 0 ? children[0] : "null") + ", " +
                (children.length > 1 ? children[1] : "null") + ")";
    }

    /**
     * Converts a Hive list to a Java list, converting each element via
     * PrimitiveObjectInspector.getPrimitiveJavaObject() when available.
     * This handles Writable types (e.g. DoubleWritable -> Double) that
     * DataTransformUtils doesn't recognize.
     */
    private List<Object> toJavaList(List<?> list, PrimitiveObjectInspector elementOI) {
        if (list == null) {
            return null;
        }
        List<Object> result = new ArrayList<>(list.size());
        for (Object o : list) {
            if (elementOI != null && o != null) {
                result.add(elementOI.getPrimitiveJavaObject(o));
            } else {
                result.add(o);
            }
        }
        return result;
    }
}
