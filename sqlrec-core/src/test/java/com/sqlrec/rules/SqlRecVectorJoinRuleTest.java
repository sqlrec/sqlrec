package com.sqlrec.rules;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.schema.VectorSearchRequest;
import com.sqlrec.common.schema.VectorSearchResult;
import com.sqlrec.common.schema.VectorSearchable;
import com.sqlrec.common.utils.FilterUtils;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import com.sqlrec.udf.UdfManager;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlRecVectorJoinRuleTest {
    private CalciteSchema schema;
    private TestVectorTable vectorTable;

    @BeforeEach
    public void setUp() throws Exception {
        vectorTable = new TestVectorTable();
        vectorTable.setTableName("vector_table");
        schema = CalciteSchema.createRootSchema(false);
        schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                Map<String, Table> tables = new HashMap<>();
                tables.put("left_table", new LeftTable());
                tables.put("vector_table", vectorTable);
                return tables;
            }
        });
        UdfManager.addFunction(
                schema.getSubSchema(Consts.DEFAULT_SCHEMA_NAME, false),
                "ip",
                "com.sqlrec.udf.scalar.IpFunction");
    }

    @Test
    public void executesLookupWithoutScanningRightTableAndPushesFilter() throws Exception {
        String sql = "select left_table.id as user_id, vector_table.id as item_id, "
                + "vector_table.category, "
                + "ip(left_table.embedding, vector_table.embedding) as score "
                + "from left_table join vector_table on 1=1 "
                + "where vector_table.category = left_table.category "
                + "order by ip(left_table.embedding, vector_table.embedding) limit 2";

        BindableInterface bindable = compile(sql);
        List<Object[]> rows = bindable.bind(schema, new ExecuteContextImpl()).toList();

        assertTrue(bindable.getPhysicalPlan().contains("SqlrecEnumerableVectorLookupJoin"));
        assertTrue(bindable.getPhysicalPlan().contains("EnumerableTableScan(table=[[default, left_table]])"));
        assertTrue(bindable.getPhysicalPlan().contains("EnumerableTableScan(table=[[default, vector_table]])"));
        assertEquals(0, vectorTable.scanCount);
        assertEquals(2, vectorTable.requests.size());
        assertEquals(2, vectorTable.requests.get(0).getTopK());
        assertEquals(4, rows.size());
        assertArrayEquals(new Object[]{1, 101, "book", 0.9d}, rows.get(0));
        assertArrayEquals(new Object[]{2, 202, "movie", 0.8d}, rows.get(3));

        List<String> rightFields = Arrays.asList("id", "category", "tags", "embedding");
        assertEquals(
                "category == \"book\"",
                FilterUtils.buildMilvusFilterExpression(
                        vectorTable.requests.get(0).getFilterCondition(),
                        vectorTable.requests.get(0).getLeftRow(),
                        rightFields));
        assertEquals(
                "category == \"movie\"",
                FilterUtils.buildMilvusFilterExpression(
                        vectorTable.requests.get(1).getFilterCondition(),
                        vectorTable.requests.get(1).getLeftRow(),
                        rightFields));
    }

    private BindableInterface compile(String sql) throws Exception {
        SqlNode sqlNode = CompileManager.parseFlinkSql(sql);
        return new CompileManager().compileSql(
                sqlNode, schema, Consts.DEFAULT_SCHEMA_NAME, sql);
    }

    public static class LeftTable extends SqlRecTable implements ScannableTable {
        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(new Object[][]{
                    {1, "book", Arrays.asList("Action"), Arrays.asList(0.1f, 0.2f)},
                    {2, "movie", Arrays.asList("Drama"), Arrays.asList(0.3f, 0.4f)}
            });
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return rowType(typeFactory);
        }
    }

    public static class TestVectorTable extends SqlRecKvTable implements VectorSearchable {
        private final List<VectorSearchRequest> requests = new ArrayList<>();
        private int scanCount;

        @Override
        protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
            scanCount++;
            throw new AssertionError("Vector lookup must not scan the right table");
        }

        @Override
        public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
            return Collections.emptyMap();
        }

        @Override
        public int getPrimaryKeyIndex() {
            return 0;
        }

        @Override
        public @Nullable Collection getModifiableCollection() {
            return new ArrayList();
        }

        @Override
        public List<VectorSearchResult> searchByEmbeddingImpl(VectorSearchRequest request) {
            requests.add(request);
            int leftId = ((Number) request.getLeftRow()[0]).intValue();
            String category = String.valueOf(request.getLeftRow()[1]);
            return Arrays.asList(
                    new VectorSearchResult(
                            new Object[]{
                                    leftId * 100 + 1,
                                    category,
                                    Arrays.asList("first"),
                                    Arrays.asList(0.5f, 0.6f)},
                            0.9d),
                    new VectorSearchResult(
                            new Object[]{
                                    leftId * 100 + 2,
                                    category,
                                    Arrays.asList("second"),
                                    Arrays.asList(0.7f, 0.8f)},
                            0.8d));
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return rowType(typeFactory);
        }
    }

    private static RelDataType rowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
                .add("id", SqlTypeName.INTEGER)
                .add("category", SqlTypeName.VARCHAR)
                .add("tags", typeFactory.createArrayType(
                        typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
                .add("embedding", typeFactory.createArrayType(
                        typeFactory.createSqlType(SqlTypeName.FLOAT), -1))
                .build();
    }
}
