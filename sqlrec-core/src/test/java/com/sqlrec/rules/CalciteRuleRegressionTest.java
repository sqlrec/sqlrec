package com.sqlrec.rules;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.compiler.NormalSqlCompiler;
import com.sqlrec.runtime.BindableInterface;
import com.sqlrec.runtime.ExecuteContextImpl;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelHintsPropagator;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CalciteRuleRegressionTest {
    private CalciteSchema rootSchema;
    private MemoryKvTable kvTable;

    @BeforeEach
    void setUp() {
        MemoryScanTable leftTable = new MemoryScanTable(new Object[][]{
                {1, "Alice"},
                {2, "Bob"},
                {4, "Dana"}
        });
        kvTable = new MemoryKvTable(new Object[][]{
                {1, "One"},
                {2, "Two"},
                {3, "Three"}
        });
        leftTable.setTableName("left_table");
        kvTable.setTableName("kv_table");

        Map<String, Table> tables = new LinkedHashMap<>();
        tables.put("left_table", leftTable);
        tables.put("right_table", new MemoryScanTable(new Object[][]{
                {1, "One"},
                {3, "Three"}
        }));
        tables.put("kv_table", kvTable);

        rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add(Consts.DEFAULT_SCHEMA_NAME, new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tables;
            }
        });
    }

    @Test
    void kvJoinRuleCreatesAndExecutesCustomJoin() throws Exception {
        BindableInterface bindable = compile(
                "select l.id, l.name, r.label "
                        + "from left_table l join kv_table r on l.id = r.id order by l.id");

        assertTrue(bindable.getPhysicalPlan().contains("SqlrecEnumerableKvJoin"));
        List<Object[]> rows = execute(bindable);
        assertEquals(2, rows.size());
        assertArrayEquals(new Object[]{1, "Alice", "One"}, rows.get(0));
        assertArrayEquals(new Object[]{2, "Bob", "Two"}, rows.get(1));
        assertEquals(Set.of(1, 2, 4), kvTable.requestedKeys);
    }

    @Test
    void kvJoinRuleRejectsUnsupportedCondition() {
        RelBuilder builder = relBuilder();
        builder.scan("left_table");
        builder.scan("kv_table");
        RexNode condition = builder.call(
                SqlStdOperatorTable.GREATER_THAN,
                builder.field(2, 0, 0),
                builder.field(2, 1, 0));
        RelNode join = builder.join(org.apache.calcite.rel.core.JoinRelType.INNER, condition).build();

        assertInstanceOf(LogicalJoin.class, join);
        assertNull(RuleManager.KV_JOIN.convert(join));
    }

    @Test
    void filterTableScanKeepsResidualPredicate() throws Exception {
        BindableInterface bindable = compile(
                "select id, label from kv_table where id > 1 and label <> 'missing' order by id");

        String plan = bindable.getPhysicalPlan();
        assertTrue(plan.contains("FilterableTableScan"));
        assertTrue(plan.contains("EnumerableCalc"));

        List<Object[]> rows = execute(bindable);
        assertFalse(kvTable.receivedFilters.isEmpty());
        assertEquals(2, rows.size());
        assertArrayEquals(new Object[]{2, "Two"}, rows.get(0));
        assertArrayEquals(new Object[]{3, "Three"}, rows.get(1));
    }

    @Test
    void ordinaryScannableTableDoesNotUseFilterableScan() throws Exception {
        BindableInterface bindable = compile(
                "select * from left_table where id > 1 order by id");

        assertFalse(bindable.getPhysicalPlan().contains("FilterableTableScan"));
        assertTrue(bindable.getPhysicalPlan().contains("EnumerableTableScan"));
    }

    @Test
    void interpreterFilterTableScanRuleUsesCustomScan() {
        RelBuilder builder = relBuilder();
        RelNode scan = builder.scan("kv_table").build();
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(scan, 1),
                rexBuilder.makeLiteral("Two"));
        RelNode root = LogicalFilter.create(
                EnumerableInterpreter.create(scan, 1D), condition);

        HepPlanner planner = new HepPlanner(new HepProgramBuilder()
                .addRuleInstance(SqlRecFilterTableScanRule.createInterpreter())
                .build());
        planner.setRoot(root);
        String plan = RelOptUtil.toString(planner.findBestExp());

        assertTrue(plan.contains("LogicalCalc"));
        assertTrue(plan.contains("FilterableTableScan"));
        assertFalse(plan.contains("EnumerableInterpreter"));
    }

    @Test
    void filterIntoJoinPushesLeftOnlyFilterBelowKvJoin() throws Exception {
        BindableInterface bindable = compile(
                "select * from left_table l join kv_table r on l.id = r.id "
                        + "where l.name is not distinct from 'Alice'");

        String plan = bindable.getPhysicalPlan();
        assertTrue(plan.contains("SqlrecEnumerableKvJoin"));
        assertTrue(plan.contains("EnumerableCalc"));
        assertTrue(plan.indexOf("SqlrecEnumerableKvJoin") < plan.indexOf("EnumerableCalc"));
        List<Object[]> rows = execute(bindable);
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{1, "Alice", 1, "One"}, rows.get(0));
    }

    @Test
    void filterIntoJoinKeepsRightFilterAboveKvJoin() throws Exception {
        BindableInterface bindable = compile(
                "select * from left_table l join kv_table r on l.id = r.id "
                        + "where r.label = 'Two'");

        String plan = bindable.getPhysicalPlan();
        assertTrue(plan.contains("SqlrecEnumerableKvJoin"));
        assertTrue(plan.contains("EnumerableCalc"));
        assertTrue(plan.indexOf("EnumerableCalc") < plan.indexOf("SqlrecEnumerableKvJoin"));
        List<Object[]> rows = execute(bindable);
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{2, "Bob", 2, "Two"}, rows.get(0));
    }

    @Test
    void nonKvJoinKeepsCalciteDefaultJoinBehavior() throws Exception {
        BindableInterface bindable = compile(
                "select * from left_table l join right_table r on l.id = r.id "
                        + "where r.name = 'One'");

        assertFalse(bindable.getPhysicalPlan().contains("SqlrecEnumerableKvJoin"));
        List<Object[]> rows = execute(bindable);
        assertEquals(1, rows.size());
        assertArrayEquals(new Object[]{1, "Alice", 1, "One"}, rows.get(0));
    }

    @Test
    void tableModifyRuleExecutesInsertUpdateAndDelete() throws Exception {
        BindableInterface insert = compile("insert into kv_table values (4, 'Four')");
        assertTrue(insert.getPhysicalPlan().contains("SqlrecEnumerableTableModify"));
        assertArrayEquals(new Object[]{1L}, execute(insert).get(0));
        assertArrayEquals(new Object[]{4, "Four"}, kvTable.row(4));

        BindableInterface update = compile(
                "update kv_table set label = 'Updated' where id = 4");
        assertTrue(update.getPhysicalPlan().contains("SqlrecEnumerableTableModify"));
        assertArrayEquals(new Object[]{0L}, execute(update).get(0));
        assertArrayEquals(new Object[]{4, "Updated"}, kvTable.row(4));

        BindableInterface delete = compile("delete from kv_table where id = 4");
        assertTrue(delete.getPhysicalPlan().contains("SqlrecEnumerableTableModify"));
        assertArrayEquals(new Object[]{1L}, execute(delete).get(0));
        assertNull(kvTable.row(4));
    }

    @Test
    void tableModifyRuleUsesRelOptTableInterface() {
        RelNode existingNode = relBuilder().scan("kv_table").build();
        RelOptTable table = relOptTableProxy(kvTable);
        RelNode node = new AbstractRelNode(
                existingNode.getCluster(), existingNode.getTraitSet()) {
            @Override
            protected RelDataType deriveRowType() {
                return rowType(getCluster().getTypeFactory());
            }

            @Override
            public RelNode copy(org.apache.calcite.plan.RelTraitSet traitSet,
                                List<RelNode> inputs) {
                return this;
            }

            @Override
            public RelOptTable getTable() {
                return table;
            }
        };

        assertTrue(RuleManager.SQLREC_TABLE_MODIFY.matches(
                new TestRuleCall(RuleManager.SQLREC_TABLE_MODIFY, node)));
    }

    private BindableInterface compile(String sql) throws Exception {
        return NormalSqlCompiler.getNormalSqlBindable(
                sql, rootSchema, Consts.DEFAULT_SCHEMA_NAME);
    }

    private List<Object[]> execute(BindableInterface bindable) {
        return bindable.bind(rootSchema, new ExecuteContextImpl()).toList();
    }

    private RelBuilder relBuilder() {
        return RelBuilder.create(Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.plus().getSubSchema(Consts.DEFAULT_SCHEMA_NAME))
                .build());
    }

    private static RelOptTable relOptTableProxy(Table table) {
        return (RelOptTable) Proxy.newProxyInstance(
                RelOptTable.class.getClassLoader(),
                new Class<?>[]{RelOptTable.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("unwrap")) {
                        Class<?> type = (Class<?>) arguments[0];
                        return type.isInstance(table) ? table : null;
                    }
                    if (method.getName().equals("toString")) {
                        return "TestRelOptTable";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static RelDataType rowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
                .add("id", SqlTypeName.INTEGER)
                .add("label", SqlTypeName.VARCHAR)
                .build();
    }

    private static final class MemoryScanTable extends SqlRecTable implements ScannableTable {
        private final Object[][] rows;

        private MemoryScanTable(Object[][] rows) {
            this.rows = rows;
        }

        @Override
        public @Nullable Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(rows);
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", SqlTypeName.INTEGER)
                    .add("name", SqlTypeName.VARCHAR)
                    .build();
        }
    }

    private static final class TestRuleCall extends RelOptRuleCall {
        private TestRuleCall(SqlRecTableModifyRule rule, RelNode rel) {
            super(rel.getCluster().getPlanner(), rule.getOperand(),
                    new RelNode[]{rel}, Collections.emptyMap());
        }

        @Override
        public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv,
                                RelHintsPropagator handler) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class MemoryKvTable extends SqlRecKvTable {
        private final Map<Integer, Object[]> rows = new LinkedHashMap<>();
        private final Collection<Object[]> modifiableRows = new RowCollection();
        private List<RexNode> receivedFilters = Collections.emptyList();
        private Set<Object> requestedKeys = Collections.emptySet();

        private MemoryKvTable(Object[][] initialRows) {
            for (Object[] row : initialRows) {
                rows.put((Integer) row[0], row);
            }
        }

        @Override
        protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
            receivedFilters = filters;
            return Linq4j.asEnumerable(new ArrayList<>(rows.values()));
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
        public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
            requestedKeys = keySet;
            Map<Object, List<Object[]>> result = new LinkedHashMap<>();
            for (Object key : keySet) {
                Object[] row = rows.get(((Number) key).intValue());
                if (row != null) {
                    result.put(key, Collections.singletonList(row));
                }
            }
            return result;
        }

        @Override
        public Collection<Object[]> getModifiableCollection() {
            return modifiableRows;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return rowType(typeFactory);
        }

        private Object[] row(int id) {
            return rows.get(id);
        }

        private final class RowCollection extends AbstractCollection<Object[]> {
            @Override
            public Iterator<Object[]> iterator() {
                return rows.values().iterator();
            }

            @Override
            public int size() {
                return rows.size();
            }

            @Override
            public boolean add(Object[] row) {
                rows.put(((Number) row[0]).intValue(), row);
                return true;
            }

            @Override
            public boolean remove(Object value) {
                if (!(value instanceof Object[] row)) {
                    return false;
                }
                return rows.remove(((Number) row[0]).intValue()) != null;
            }

            @Override
            public boolean removeAll(Collection<?> values) {
                boolean changed = false;
                for (Object value : values) {
                    changed |= remove(value);
                }
                return changed;
            }
        }
    }
}
