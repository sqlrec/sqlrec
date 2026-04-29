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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomVecFunction extends GenericUDF {
    private static final Random random = new Random();
    private StringObjectInspector dimensionOI;
    private ListObjectInspector returnOI;

    public List<Double> evaluate(String dimensionStr) {
        if (dimensionStr == null) {
            return null;
        }

        int dimension = parseDimension(dimensionStr);
        return generateRandomVector(dimension);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("RandomVecFunction requires exactly one argument.");
        }

        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "The argument must be a string.");
        }

        this.dimensionOI = (StringObjectInspector) arguments[0];

        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        return ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null || arguments[0].get() == null) {
            return null;
        }

        String dimensionStr = dimensionOI.getPrimitiveJavaObject(arguments[0].get());
        if (dimensionStr == null) {
            return null;
        }

        try {
            int dimension = parseDimension(dimensionStr);
            return generateRandomVector(dimension);
        } catch (RuntimeException e) {
            throw new HiveException(e.getMessage());
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "random_vec(" + (children.length > 0 ? children[0] : "null") + ")";
    }

    private int parseDimension(String dimensionStr) {
        int dimension;
        try {
            dimension = Integer.parseInt(dimensionStr);
        } catch (NumberFormatException e) {
            throw new RuntimeException("RandomVecFunction dimension must be a valid integer string");
        }

        if (dimension <= 0) {
            throw new RuntimeException("RandomVecFunction dimension must be positive");
        }

        return dimension;
    }

    private List<Double> generateRandomVector(int dimension) {
        List<Double> vector = new ArrayList<>();
        double sum = 0;

        for (int i = 0; i < dimension; i++) {
            double value = random.nextDouble();
            vector.add(value);
            sum += value * value;
        }

        if (sum <= 0) {
            return vector;
        }

        double norm = Math.sqrt(sum);
        List<Double> result = new ArrayList<>();
        for (Double value : vector) {
            result.add(value / norm);
        }

        return result;
    }
}
