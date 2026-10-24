package com.sqlrec.schema;

import com.google.common.collect.ImmutableList;
import com.sqlrec.udf.SqlRecScalarFunctionImpl;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import java.util.List;
import java.util.Objects;

// make cache table has higher priority than table in hms
public class RootFirstCatalogReader extends CalciteCatalogReader {
    public RootFirstCatalogReader(CalciteSchema rootSchema,
                                List<String> defaultSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
        super(rootSchema, SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
                ImmutableList.of(
                        ImmutableList.of(),
                        Objects.requireNonNull(defaultSchema, "defaultSchema")
                ),
                typeFactory, config);
    }

    @Override
    public void lookupOperatorOverloads(SqlIdentifier name, SqlFunctionCategory category, SqlSyntax syntax,
                                        List<SqlOperator> operators, SqlNameMatcher matcher) {
        int start = operators.size();
        super.lookupOperatorOverloads(name, category, syntax, operators, matcher);
        for (int i = start; i < operators.size(); i++) {
            SqlOperator operator = operators.get(i);
            if (operator instanceof SqlUserDefinedFunction) {
                SqlUserDefinedFunction function = (SqlUserDefinedFunction) operator;
                if (function.getFunction() instanceof SqlRecScalarFunctionImpl
                        && !((SqlRecScalarFunctionImpl) function.getFunction()).isDeterministic()) {
                    // Calcite 1.32 does not propagate Hive's UDFType to the SQL operator.
                    // Mark it before SQL-to-Rex conversion so remote calls are never folded.
                    operators.set(i, new SqlUserDefinedFunction(function.getNameAsId(), function.getKind(),
                            function.getReturnTypeInference(), function.getOperandTypeInference(),
                            function.getOperandTypeChecker(), function.getFunction()) {
                        @Override
                        public boolean isDeterministic() {
                            return false;
                        }
                    });
                }
            }
        }
    }
}
