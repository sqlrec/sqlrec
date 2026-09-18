package com.sqlrec.connectors.jdbc.sql;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JdbcFilterBuilderTest {
    private RexBuilder rexBuilder;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
    }

    // --- SQL filter tests (parameterized) ---

    private List<FieldSchema> sqlFieldSchemas = Arrays.asList(
            new FieldSchema("id", "INTEGER"),
            new FieldSchema("name", "VARCHAR"),
            new FieldSchema("age", "INTEGER")
    );

    private static final String PG_URL = "jdbc:postgresql://localhost:5432/test";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/test";

    @Test
    public void testBuildSqlFilter_Empty() {
        JdbcStatement filter = JdbcFilterBuilder.build(Collections.emptyList(), sqlFieldSchemas, PG_URL);
        assertEquals("", filter.getSql());
        assertTrue(filter.getParameters().isEmpty());
    }

    @Test
    public void testBuildSqlFilter_Null() {
        JdbcStatement filter = JdbcFilterBuilder.build(null, sqlFieldSchemas, PG_URL);
        assertEquals("", filter.getSql());
        assertTrue(filter.getParameters().isEmpty());
    }

    @Test
    public void testBuildSqlFilter_Equals() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexLiteral literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("id = ?", result.getSql());
        assertEquals(Collections.singletonList(new java.math.BigDecimal(1)), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_StringEquals() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        // The value must NOT be interpolated into the SQL text.
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList("Bob"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_ValueWithSingleQuoteIsBoundAsParameter() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("O'Brien", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList("O'Brien"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_InjectionAttemptSingleQuote() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("'; DROP TABLE users; --", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList("'; DROP TABLE users; --"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_InjectionAttemptBackslashQuote() {
        // On MySQL (default sql_mode) '\' escapes the following quote inside string
        // literals, so quote-doubling alone cannot stop this payload from breaking out
        // of the literal. Parameter binding makes it inert.
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("\\'; DROP TABLE users; --", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, MYSQL_URL);
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList("\\'; DROP TABLE users; --"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_InjectionAttemptAlwaysTrue() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("x' OR '1'='1", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList("x' OR '1'='1"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_GreaterThanOrEqual() {
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(25));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ref2, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("age >= ?", result.getSql());
        assertEquals(Collections.singletonList(new java.math.BigDecimal(25)), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_AndCondition() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeExactLiteral(new java.math.BigDecimal(2));
        RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal1);

        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal2 = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal2);

        RexNode andFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(andFilter), sqlFieldSchemas, PG_URL);
        assertEquals("(id = ?) AND (name = ?)", result.getSql());
        // parameters follow placeholder order
        assertEquals(Arrays.asList(new java.math.BigDecimal(2), "Bob"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_OrCondition() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal1);

        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal2 = rexBuilder.makeLiteral("Alice", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal2);

        RexNode orFilter = rexBuilder.makeCall(SqlStdOperatorTable.OR, cond1, cond2);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(orFilter), sqlFieldSchemas, PG_URL);
        assertEquals("(id = ?) OR (name = ?)", result.getSql());
        assertEquals(Arrays.asList(new java.math.BigDecimal(1), "Alice"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_MultipleFilters() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Arrays.asList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("id = ?", result.getSql());
        assertEquals(Collections.singletonList(new java.math.BigDecimal(1)), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_UnsafeFieldNameQuoted() {
        // A field name coming from (untrusted) table metadata must not be able to break
        // out of the WHERE clause; unsafe identifiers are quoted like the SELECT list.
        List<FieldSchema> evilSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name\"; DROP TABLE users; --", "VARCHAR")
        );
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        JdbcStatement pgResult = JdbcFilterBuilder.build(Collections.singletonList(filter), evilSchemas, PG_URL);
        assertEquals("\"name\"\"; DROP TABLE users; --\" = ?", pgResult.getSql());
        assertEquals(Collections.singletonList("Bob"), pgResult.getParameters());

        JdbcStatement mysqlResult = JdbcFilterBuilder.build(Collections.singletonList(filter), evilSchemas, MYSQL_URL);
        assertEquals("`name\"; DROP TABLE users; --` = ?", mysqlResult.getSql());
    }

    @Test
    public void testBuildSqlFilter_SafeFieldNameNotQuoted() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        // safe identifiers stay unquoted to preserve case-folding behavior
        assertEquals("id = ?", result.getSql());
    }

    @Test
    public void testBuildSqlFilter_NullLiteralBoundAsNullParameter() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode nullLiteral = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, nullLiteral);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList(null), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_IsNull() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ref1);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("name IS NULL", result.getSql());
        assertTrue(result.getParameters().isEmpty());
    }

    @Test
    public void testBuildSqlFilter_IsNotNull() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ref1);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("name IS NOT NULL", result.getSql());
    }

    @Test
    public void testBuildSqlFilter_ArrayContainsParameterized() {
        List<FieldSchema> arrayFieldSchemas = Collections.singletonList(
                new FieldSchema("tags", "ARRAY")
        );
        RexInputRef tagsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode literal = rexBuilder.makeLiteral("a", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(ARRAY_CONTAINS, tagsRef, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), arrayFieldSchemas, PG_URL);
        assertEquals("tags @> ARRAY[?]", result.getSql());
        assertEquals(Collections.singletonList("a"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_ArrayContainsAllWithArrayConstructor() {
        List<FieldSchema> arrayFieldSchemas = Collections.singletonList(
                new FieldSchema("tags", "ARRAY")
        );
        RexInputRef tagsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode elem1 = rexBuilder.makeLiteral("a", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode elem2 = rexBuilder.makeLiteral("b'", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode array = rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, elem1, elem2);
        RexNode filter = rexBuilder.makeCall(ARRAY_CONTAINS_ALL, tagsRef, array);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), arrayFieldSchemas, PG_URL);
        assertEquals("array_contains_all(tags, ?, ?)", result.getSql());
        assertEquals(Arrays.asList("a", "b'"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_ReversedLiteralAndField() {
        // literal on the left, field on the right: placeholder must still come first
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, literal, ref1);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("? = name", result.getSql());
        assertEquals(Collections.singletonList("Bob"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_DecimalLiteralKeepsPrecision() {
        // getValue3() must keep decimals as BigDecimal; getValue2() would truncate to Long
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal("10.5"));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref2, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("age > ?", result.getSql());
        assertEquals(Collections.singletonList(new java.math.BigDecimal("10.5")), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_BooleanLiteral() {
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 2);
        RexNode literal = rexBuilder.makeLiteral(true);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref2, literal);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("age = ?", result.getSql());
        assertEquals(Collections.singletonList(Boolean.TRUE), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_RejectsUnsupportedOperandKind() {
        // a nested arithmetic call is not a supported operand and must fail fast
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, ref0,
                rexBuilder.makeExactLiteral(new java.math.BigDecimal(1)));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, plus,
                rexBuilder.makeExactLiteral(new java.math.BigDecimal(2)));

        assertThrows(IllegalArgumentException.class,
                () -> JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL));
    }

    @Test
    public void testBuildSqlFilter_NotBooleanField() {
        // Unary NOT is rendered as a SQL predicate instead of being treated as a binary filter.
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 0);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.NOT, ref0);

        JdbcStatement result = JdbcFilterBuilder.build(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("NOT (id)", result.getSql());
    }


    private static final SqlOperator ARRAY_CONTAINS = new SqlFunction(
            "ARRAY_CONTAINS",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);

    private static final SqlOperator ARRAY_CONTAINS_ALL = new SqlFunction(
            "ARRAY_CONTAINS_ALL",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);
}

