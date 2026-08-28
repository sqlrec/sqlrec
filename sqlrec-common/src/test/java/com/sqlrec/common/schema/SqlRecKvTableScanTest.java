package com.sqlrec.common.schema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlRecKvTableScanTest {

    @Test
    void primaryKeyOnlyTableExtractsCandidateFromAndCondition() {
        List<RexNode> filters = createAndFilters();
        CandidateTable table = new CandidateTable(true);

        List<Object[]> rows = table.scan(null, filters).toList();

        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals(Collections.singleton(1), table.requestedKeys);
        assertFalse(table.scanImplCalled);
        assertEquals(1, filters.size(), "the complete filter must remain available to the residual calc");
    }

    @Test
    void generalTableReceivesCompleteCondition() {
        List<RexNode> filters = createAndFilters();
        CandidateTable table = new CandidateTable(false);

        table.scan(null, filters).toList();

        assertTrue(table.scanImplCalled);
        assertSame(filters, table.receivedFilters);
        assertTrue(table.requestedKeys.isEmpty());
    }

    @Test
    void primaryKeyOnlyTableIgnoresNonPrimaryKeyConditionWhenFetchingCandidates() {
        CandidateTable table = new CandidateTable(true);
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        List<RexNode> filters = new ArrayList<>();
        filters.add(rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(20))));

        table.scan(null, filters).toList();

        assertNotSame(filters, table.receivedFilters);
        assertTrue(table.receivedFilters.isEmpty());
        assertTrue(table.requestedKeys.isEmpty());
    }

    private List<RexNode> createAndFilters() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RexInputRef id = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexInputRef name = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode idEquals = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, id,
                rexBuilder.makeExactLiteral(BigDecimal.ONE));
        RexNode nameEquals = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, name,
                rexBuilder.makeLiteral("Alice"));
        return new ArrayList<>(Collections.singletonList(
                rexBuilder.makeCall(SqlStdOperatorTable.AND, idEquals, nameEquals)));
    }

    private static final class CandidateTable extends SqlRecKvTable {
        private final boolean primaryKeyOnly;
        private boolean scanImplCalled;
        private List<RexNode> receivedFilters;
        private Set<Object> requestedKeys = Collections.emptySet();

        private CandidateTable(boolean primaryKeyOnly) {
            this.primaryKeyOnly = primaryKeyOnly;
        }

        @Override
        protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
            scanImplCalled = true;
            receivedFilters = filters;
            return Linq4j.asEnumerable(Collections.singletonList(new Object[]{2, "Bob"}));
        }

        @Override
        public int getPrimaryKeyIndex() {
            return 0;
        }

        @Override
        public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
            requestedKeys = keySet;
            return Collections.singletonMap(1,
                    Collections.singletonList(new Object[]{1, "Alice"}));
        }

        @Override
        public boolean onlyFilterByPrimaryKey() {
            return primaryKeyOnly;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", SqlTypeName.INTEGER)
                    .add("name", SqlTypeName.VARCHAR)
                    .build();
        }

        @Override
        public Collection<Object[]> getModifiableCollection() {
            return new ArrayList<>();
        }
    }
}
