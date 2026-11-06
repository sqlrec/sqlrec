package com.sqlrec.node;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlrecEnumerableKvJoinTest {
    @Test
    void rightJoinPreservesUnmatchedRightRows() throws Exception {
        assertJoin("RIGHT", List.of(1, 2), List.of("[1, 1]", "[null, 3]"));
    }

    @Test
    void fullJoinPreservesUnmatchedRowsFromBothInputs() throws Exception {
        assertJoin("FULL", List.of(1, 2), List.of("[1, 1]", "[2, null]", "[null, 3]"));
    }

    @Test
    void outerJoinsPreserveRightRowsWhenLeftIsEmpty() throws Exception {
        for (String join : List.of("RIGHT", "FULL")) {
            assertJoin(join, List.of(), List.of("[null, 1]", "[null, 3]"));
        }
    }

    @Test
    void innerAndLeftJoinsStillUseKeyLookups() throws Exception {
        assertJoin("INNER", List.of(1, 2), List.of("[1, 1]"));
        assertJoin("LEFT", List.of(1, 2), List.of("[1, 1]", "[2, null]"));
    }

    private void assertJoin(String join, List<Integer> leftIds, List<String> expected) throws Exception {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        CalciteSchema database = schema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema());
        var fields = new JavaTypeFactoryImpl().builder().add("ID", SqlTypeName.INTEGER).build().getFieldList();
        database.add("L", new CacheTable("L", Linq4j.asEnumerable(
                leftIds.stream().map(id -> new Object[]{id}).toList()), fields));
        var right = new TestKvTable();
        database.add("R", right);
        var bindable = NormalSqlCompiler.getNormalSqlBindable(
                "SELECT L.ID AS LI, R.ID AS RI FROM L " + join + " JOIN R ON L.ID = R.ID",
                schema, Consts.DEFAULT_SCHEMA_NAME);
        List<String> actual = bindable.bind(schema, new ExecuteContextImpl(schema)).toList().stream()
                .map(Arrays::toString).sorted().toList();
        assertEquals(expected.stream().sorted().toList(), actual);
        assertEquals(join.equals("INNER") || join.equals("LEFT"), right.keyLookupUsed);
    }

    private static class TestKvTable extends SqlRecKvTable {
        private final List<Object[]> rows = Arrays.asList(new Object[]{1}, new Object[]{3});
        private boolean keyLookupUsed;

        TestKvTable() {
            setTableName("R");
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory factory) {
            return factory.builder().add("ID", SqlTypeName.INTEGER).build();
        }

        @Override
        protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
            return Linq4j.asEnumerable(rows);
        }

        @Override
        public boolean onlyFilterByPrimaryKey() {
            return false;
        }

        @Override
        public int getPrimaryKeyIndex() {
            return 0;
        }

        @Override
        public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keys) {
            keyLookupUsed = true;
            return rows.stream().filter(row -> keys.contains(row[0]))
                    .collect(Collectors.groupingBy(row -> row[0]));
        }

        @Override
        public Collection<?> getModifiableCollection() {
            throw new UnsupportedOperationException();
        }
    }
}
