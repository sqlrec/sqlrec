package com.sqlrec.schema;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CalciteSchemaTest {
    public static void main(String[] args) throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("t0", new MyTable("t0", new Object[][]{{1, "Alice"}}));
            }
        });
        schema.add("t0", new MyTable("t0", new Object[][]{{2, "Bob"}}));
        schema.add("t2", new MyTable("t2", new Object[][]{{3, "Charlie"}}));

        CalciteSchema.TableEntry entry = schema.getTable("t0", false);
        System.out.println(entry);

        HmsSchema.setGlobalSchema(schema);

        List<String> sqlList = Arrays.asList(
                "select * from t0",
                "select * from t2"
        );

        for (String sql : sqlList) {
            System.out.println("\n" + sql);
            SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
            BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

            Enumerable enumerable = bindable.bind(schema);
            if (enumerable != null) {
                List<Object[]> results = enumerable.toList();
                for (Object[] result : results) {
                    System.out.println(java.util.Arrays.toString(result));
                }
            }
        }
    }

    public static class MyTable extends SqlRecTable implements ScannableTable {
        public String name;
        public Object[][] data;

        public MyTable(String name, Object[][] data) {
            this.name = name;
            this.data = data;
        }

        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(data);
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", SqlTypeName.INTEGER)
                    .add("NAME", SqlTypeName.VARCHAR, 20)
                    .build();
        }

        @Override
        public SqlRecTableType getSqlRecTableType() {
            return SqlRecTableType.MEMORY;
        }
    }
}
