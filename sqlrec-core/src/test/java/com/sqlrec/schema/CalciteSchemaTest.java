package com.sqlrec.schema;

import com.sqlrec.common.schema.ExecuteContext;
import com.sqlrec.common.schema.SqlRecTable;
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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CalciteSchemaTest {
    @Test
    public void testTablePriority() throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add(NormalSqlCompiler.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Collections.singletonMap("t0", new MyTable("t0", new Object[][]{{1, "Alice"}}));
            }
        });
        schema.add("t0", new MyTable("t0", new Object[][]{{2, "Bob"}}));

        HmsSchema.setGlobalSchema(schema);

        String sql = "select * from t0";

        System.out.println("\n" + sql);
        SqlNode flinkSqlNode = CompileManager.parseFlinkSql(sql);
        BindableInterface bindable = CompileManager.compileSql(flinkSqlNode, schema, NormalSqlCompiler.DEFAULT_SCHEMA_NAME);

        Enumerable enumerable = bindable.bind(schema, new ExecuteContext());
        assert enumerable != null;
        List<Object[]> results = enumerable.toList();
        assert results.size() == 1;
        Object[] result = results.get(0);
        System.out.println(Arrays.toString(result));
        assert result.length == 2;
        assert result[0].equals(2);
        assert result[1].equals("Bob");
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
    }
}
