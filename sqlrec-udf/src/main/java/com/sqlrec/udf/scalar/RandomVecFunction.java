package com.sqlrec.udf.scalar;

import com.sqlrec.common.utils.VectorMathUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@UDFType(deterministic = false)
public class RandomVecFunction extends GenericUDF {
    private static final Random random = new Random();
    private IntObjectInspector dimensionOI;

    public List<Double> evaluate(Integer dimension) {
        if (dimension == null) {
            return null;
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("RandomVecFunction dimension must be positive");
        }
        return generateRandomVector(dimension);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("RandomVecFunction requires exactly one argument.");
        }

        if (!(arguments[0] instanceof IntObjectInspector)) {
            throw new UDFArgumentTypeException(0, "The argument must be an INT.");
        }

        this.dimensionOI = (IntObjectInspector) arguments[0];

        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        return ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0] == null || arguments[0].get() == null) {
            return null;
        }

        try {
            return evaluate(dimensionOI.get(arguments[0].get()));
        } catch (RuntimeException e) {
            throw new HiveException(e.getMessage());
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "random_vec(" + (children.length > 0 ? children[0] : "null") + ")";
    }

    private List<Double> generateRandomVector(int dimension) {
        List<Double> vector = new ArrayList<>(dimension);
        for (int i = 0; i < dimension; i++) {
            vector.add(random.nextDouble());
        }
        return VectorMathUtils.l2NormalizeList(vector);
    }
}
