package com.sqlrec;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.Lex;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Frameworks;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.Map;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

public class TestCalciteSql {

    public static RelTraitSet getDesiredRootTraitSet(RelRoot root) {
        // Make sure non-CallingConvention traits, if any, are preserved
        return root.rel.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)
                .replace(root.collation)
                .simplify();
    }

    public static void main(String[] args) throws SqlParseException {
        // Define a schema (as in the previous example)
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("mySchema", new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("t1", new MyTable());
            }
        });

        // Configure SQL parser
        SqlParser.Config parserConfig = SqlParser.config()
                .withConformance(SqlConformanceEnum.DEFAULT)
                .withParserFactory(FlinkSqlParserImpl.FACTORY);

        // SQL query to parse
//        String sql = "cache table t2 as select * from t1 where id=1";
//        String sql = "cache table t2 as fun1(t1)";
        String sql = "return t1";

        // Parse the SQL query
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseQuery();
        System.out.println(sqlNode);

//        // Validate the SQL query
//        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
//                CalciteSchema.from(rootSchema),
//                Collections.singletonList("mySchema"),
//                new JavaTypeFactoryImpl(),
//                null);
//        SqlValidator validator = SqlValidatorUtil.newValidator(
//                Frameworks.newConfigBuilder().build().getOperatorTable(),
//                catalogReader,
//                new JavaTypeFactoryImpl(),
//                SqlValidator.Config.DEFAULT
//        );
//        SqlNode validatedSqlNode = validator.validate(sqlNode);
    }

    public static class MyTable extends AbstractTable implements ScannableTable {

        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, "Alice"},
                    {2, "Bob"},
                    {3, "Charlie"}
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", SqlTypeName.INTEGER)
                    .add("NAME", SqlTypeName.VARCHAR, 20)
                    .build();
        }
    }
}