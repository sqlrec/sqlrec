package com.sqlrec.common.utils;

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

public class FilterUtilsTest {

    private RexBuilder rexBuilder;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    public void setUp() {
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
    }

    @Test
    public void testBuildMilvusFilterExpression_NullCondition() {
        Object[] leftValue = new Object[]{"value1", 123};
        List<String> rightFieldNames = Arrays.asList("field1", "field2");

        String result = FilterUtils.buildMilvusFilterExpression(null, leftValue, rightFieldNames);
        assertNull(result);
    }

    @Test
    public void testBuildMilvusFilterExpression_NullLeftValue() {
        RexInputRef leftRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexInputRef rightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);
        List<String> rightFieldNames = Arrays.asList("field1", "field2");

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, null, rightFieldNames);
        assertEquals("field1 == field2", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_SimpleEquals() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createEqualsCondition(0, 2);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("category == \"category1\"", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_SimpleGreaterThan() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createComparisonCondition(SqlStdOperatorTable.GREATER_THAN, 1, 3);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("price > 100", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_SimpleLessThan() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createComparisonCondition(SqlStdOperatorTable.LESS_THAN, 1, 3);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("price < 100", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_SimpleGreaterThanOrEqual() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createComparisonCondition(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, 1, 3);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("price >= 100", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_SimpleLessThanOrEqual() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createComparisonCondition(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, 1, 3);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("price <= 100", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_SimpleNotEquals() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createComparisonCondition(SqlStdOperatorTable.NOT_EQUALS, 0, 2);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("category <> \"category1\"", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_AndCondition() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode leftCond = createEqualsCondition(0, 2);
        RexNode rightCond = createComparisonCondition(SqlStdOperatorTable.GREATER_THAN, 1, 3);
        RexNode andCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, leftCond, rightCond);

        String result = FilterUtils.buildMilvusFilterExpression(andCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("(category == \"category1\" and price > 100)", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_OrCondition() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode leftCond = createEqualsCondition(0, 2);
        RexNode rightCond = createEqualsCondition(0, 3);
        RexNode orCondition = rexBuilder.makeCall(SqlStdOperatorTable.OR, leftCond, rightCond);

        String result = FilterUtils.buildMilvusFilterExpression(orCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("(category == \"category1\" or price == \"category1\")", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_RightFieldWithLiteral() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexInputRef fieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
        RexNode literal = rexBuilder.makeLiteral("test_value", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertTrue(result.startsWith("category == "));
        assertTrue(result.contains("test_value"));
    }

    @Test
    public void testBuildMilvusFilterExpression_LiteralWithRightField() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode literal = rexBuilder.makeLiteral("test_value", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexInputRef fieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 3);
        RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, literal, fieldRef);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertTrue(result.endsWith(" == price"));
        assertTrue(result.contains("test_value"));
    }

    @Test
    public void testBuildMilvusFilterExpression_RightFieldToRightField() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexInputRef leftFieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
        RexInputRef rightFieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 3);
        RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftFieldRef, rightFieldRef);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("category == price", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_ReversedOperands() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createComparisonConditionReversed(SqlStdOperatorTable.GREATER_THAN, 0, 2);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("category < \"category1\"", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_IntegerValue() {
        Object[] leftValue = new Object[]{12345, 100};
        List<String> rightFieldNames = Arrays.asList("user_id", "price");

        RexNode filterCondition = createEqualsCondition(0, 2);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("user_id == 12345", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_ComplexAndOrCondition() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode cond1 = createEqualsCondition(0, 2);
        RexNode cond2 = createComparisonCondition(SqlStdOperatorTable.GREATER_THAN, 1, 3);
        RexNode andCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2);

        RexNode cond3 = createEqualsCondition(0, 3);
        RexNode orCond = rexBuilder.makeCall(SqlStdOperatorTable.OR, andCond, cond3);

        String result = FilterUtils.buildMilvusFilterExpression(orCond, leftValue, rightFieldNames);

        assertNotNull(result);
        assertTrue(result.contains("or"));
        assertTrue(result.contains("and"));
    }

    @Test
    public void testBuildMilvusFilterExpression_LeftFieldOnlyCondition() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexInputRef leftFieldRef1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexInputRef leftFieldRef2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
        RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftFieldRef1, leftFieldRef2);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNull(result);
    }

    private RexNode createEqualsCondition(int leftIndex, int rightIndex) {
        RexInputRef leftRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), leftIndex);
        RexInputRef rightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), rightIndex);
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);
    }

    private RexNode createComparisonCondition(SqlOperator op, int leftIndex, int rightIndex) {
        RexInputRef leftRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), leftIndex);
        RexInputRef rightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), rightIndex);
        return rexBuilder.makeCall(op, leftRef, rightRef);
    }

    private RexNode createComparisonConditionReversed(SqlOperator op, int leftIndex, int rightIndex) {
        RexInputRef leftRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), rightIndex);
        RexInputRef rightRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), leftIndex);
        return rexBuilder.makeCall(op, leftRef, rightRef);
    }

    // --- Milvus filter tests ---

    private List<FieldSchema> milvusFieldSchemas = Arrays.asList(
            new FieldSchema("id", "INTEGER"),
            new FieldSchema("name", "VARCHAR"),
            new FieldSchema("age", "INTEGER")
    );

    @Test
    public void testGetMilvusFilterSqlString_Empty() {
        assertEquals("", FilterUtils.getMilvusFilterSqlString(Collections.emptyList(), milvusFieldSchemas));
    }

    @Test
    public void testGetMilvusFilterSqlString_Null() {
        assertEquals("", FilterUtils.getMilvusFilterSqlString((List<RexNode>) null, milvusFieldSchemas));
    }

    @Test
    public void testGetMilvusFilterSqlString_Equals() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexLiteral literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("id == 1", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_StringEquals() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("name == \"Bob\"", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_GreaterThan() {
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(25));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref2, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("age > 25", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_LessThan() {
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(30));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref2, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("age < 30", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_GreaterThanOrEqual() {
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(25));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ref2, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("age >= 25", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_LessThanOrEqual() {
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(60));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, ref2, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("age <= 60", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_NotEquals() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, ref0, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("id <> 1", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_AndCondition() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeExactLiteral(new java.math.BigDecimal(2));
        RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal1);

        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal2 = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal2);

        RexNode andFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(andFilter), milvusFieldSchemas);
        assertEquals("(id == 2) AND (name == \"Bob\")", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_OrCondition() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal1);

        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal2 = rexBuilder.makeLiteral("Alice", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal2);

        RexNode orFilter = rexBuilder.makeCall(SqlStdOperatorTable.OR, cond1, cond2);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(orFilter), milvusFieldSchemas);
        assertEquals("(id == 1) OR (name == \"Alice\")", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_MultipleFilters() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Arrays.asList(filter), milvusFieldSchemas);
        assertEquals("id == 1", result);
    }

    // --- Milvus special character escaping tests ---

    @Test
    public void testGetMilvusFilterSqlString_StringWithDoubleQuote() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Tom\"in", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("name == \"Tom\\\"in\"", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_StringWithBackslash() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("C:\\path", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("name == \"C:\\\\path\"", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_StringWithBothSpecialChars() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("a\"b\\c", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("name == \"a\\\"b\\\\c\"", result);
    }

    @Test
    public void testGetMilvusFilterSqlString_StringWithoutSpecialChars() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("plain_value", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("name == \"plain_value\"", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_StringWithDoubleQuote() {
        Object[] leftValue = new Object[]{"Tom\"in", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createEqualsCondition(0, 2);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("category == \"Tom\\\"in\"", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_StringWithBackslash() {
        Object[] leftValue = new Object[]{"C:\\path", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createEqualsCondition(0, 2);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("category == \"C:\\\\path\"", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_StringWithBothSpecialChars() {
        Object[] leftValue = new Object[]{"a\"b\\c", 100};
        List<String> rightFieldNames = Arrays.asList("category", "price");

        RexNode filterCondition = createEqualsCondition(0, 2);

        String result = FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, rightFieldNames);

        assertNotNull(result);
        assertEquals("category == \"a\\\"b\\\\c\"", result);
    }

    // --- Milvus field name validation tests ---

    @Test
    public void testGetMilvusFilterSqlString_RejectsUnsafeFieldName() {
        // Milvus filter syntax has no identifier quoting, so a field name that could
        // inject expression syntax must be rejected instead of being inlined.
        List<FieldSchema> evilSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name\" or \"1\" == \"1", "VARCHAR")
        );
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        assertThrows(IllegalArgumentException.class,
                () -> FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), evilSchemas));
    }

    @Test
    public void testGetMilvusFilterSqlString_RejectsNullFieldName() {
        List<FieldSchema> evilSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema(null, "VARCHAR")
        );
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        assertThrows(IllegalArgumentException.class,
                () -> FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), evilSchemas));
    }

    @Test
    public void testGetMilvusFilterSqlString_AcceptsSafeFieldName() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        String result = FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas);
        assertEquals("name == \"Bob\"", result);
    }

    @Test
    public void testBuildMilvusFilterExpression_RejectsUnsafeFieldName() {
        Object[] leftValue = new Object[]{"category1", 100};
        List<String> evilFieldNames = Arrays.asList("category", "price or true");

        RexNode filterCondition = createEqualsCondition(0, 2);

        assertThrows(IllegalArgumentException.class,
                () -> FilterUtils.buildMilvusFilterExpression(filterCondition, leftValue, evilFieldNames));
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
        SqlStatement filter = FilterUtils.buildSqlFilter(Collections.emptyList(), sqlFieldSchemas, PG_URL);
        assertEquals("", filter.getSql());
        assertTrue(filter.getParameters().isEmpty());
    }

    @Test
    public void testBuildSqlFilter_Null() {
        SqlStatement filter = FilterUtils.buildSqlFilter(null, sqlFieldSchemas, PG_URL);
        assertEquals("", filter.getSql());
        assertTrue(filter.getParameters().isEmpty());
    }

    @Test
    public void testBuildSqlFilter_Equals() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexLiteral literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("id = ?", result.getSql());
        assertEquals(Collections.singletonList(new java.math.BigDecimal(1)), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_StringEquals() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        // The value must NOT be interpolated into the SQL text.
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList("Bob"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_ValueWithSingleQuoteIsBoundAsParameter() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("O'Brien", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList("O'Brien"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_InjectionAttemptSingleQuote() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("'; DROP TABLE users; --", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
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

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, MYSQL_URL);
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList("\\'; DROP TABLE users; --"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_InjectionAttemptAlwaysTrue() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("x' OR '1'='1", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList("x' OR '1'='1"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_GreaterThanOrEqual() {
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(25));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ref2, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
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

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(andFilter), sqlFieldSchemas, PG_URL);
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

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(orFilter), sqlFieldSchemas, PG_URL);
        assertEquals("(id = ?) OR (name = ?)", result.getSql());
        assertEquals(Arrays.asList(new java.math.BigDecimal(1), "Alice"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_MultipleFilters() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Arrays.asList(filter), sqlFieldSchemas, PG_URL);
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

        SqlStatement pgResult = FilterUtils.buildSqlFilter(Collections.singletonList(filter), evilSchemas, PG_URL);
        assertEquals("\"name\"\"; DROP TABLE users; --\" = ?", pgResult.getSql());
        assertEquals(Collections.singletonList("Bob"), pgResult.getParameters());

        SqlStatement mysqlResult = FilterUtils.buildSqlFilter(Collections.singletonList(filter), evilSchemas, MYSQL_URL);
        assertEquals("`name\"; DROP TABLE users; --` = ?", mysqlResult.getSql());
    }

    @Test
    public void testBuildSqlFilter_SafeFieldNameNotQuoted() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        // safe identifiers stay unquoted to preserve case-folding behavior
        assertEquals("id = ?", result.getSql());
    }

    @Test
    public void testBuildSqlFilter_NullLiteralBoundAsNullParameter() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode nullLiteral = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, nullLiteral);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("name = ?", result.getSql());
        assertEquals(Collections.singletonList(null), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_ArrayContainsParameterized() {
        List<FieldSchema> arrayFieldSchemas = Collections.singletonList(
                new FieldSchema("tags", "ARRAY")
        );
        RexInputRef tagsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode literal = rexBuilder.makeLiteral("a", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(ARRAY_CONTAINS, tagsRef, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), arrayFieldSchemas, PG_URL);
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

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), arrayFieldSchemas, PG_URL);
        assertEquals("array_contains_all(tags, ?, ?)", result.getSql());
        assertEquals(Arrays.asList("a", "b'"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_ReversedLiteralAndField() {
        // literal on the left, field on the right: placeholder must still come first
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, literal, ref1);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("? = name", result.getSql());
        assertEquals(Collections.singletonList("Bob"), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_DecimalLiteralKeepsPrecision() {
        // getValue3() must keep decimals as BigDecimal; getValue2() would truncate to Long
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal("10.5"));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref2, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
        assertEquals("age > ?", result.getSql());
        assertEquals(Collections.singletonList(new java.math.BigDecimal("10.5")), result.getParameters());
    }

    @Test
    public void testBuildSqlFilter_BooleanLiteral() {
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 2);
        RexNode literal = rexBuilder.makeLiteral(true);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref2, literal);

        SqlStatement result = FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL);
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
                () -> FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL));
    }

    @Test
    public void testBuildSqlFilter_RejectsNonBinaryFilter() {
        // NOT is a unary operator: no meaningful SQL rendering, must fail fast
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 0);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.NOT, ref0);

        assertThrows(IllegalArgumentException.class,
                () -> FilterUtils.buildSqlFilter(Collections.singletonList(filter), sqlFieldSchemas, PG_URL));
    }

    @Test
    public void testGetMilvusFilterSqlString_ArrayContains() {
        List<FieldSchema> arrayFieldSchemas = Collections.singletonList(
                new FieldSchema("tags", "ARRAY")
        );
        RexInputRef tagsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode literal = rexBuilder.makeLiteral("a", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(ARRAY_CONTAINS, tagsRef, literal);

        assertEquals("array_contains(tags, \"a\")",
                FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), arrayFieldSchemas));
    }

    @Test
    public void testGetMilvusFilterSqlString_ArrayContainsWithArrayConstructor() {
        List<FieldSchema> arrayFieldSchemas = Collections.singletonList(
                new FieldSchema("tags", "ARRAY")
        );
        RexInputRef tagsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode elem1 = rexBuilder.makeLiteral("a", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode elem2 = rexBuilder.makeLiteral("b\"", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode array = rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, elem1, elem2);
        RexNode filter = rexBuilder.makeCall(ARRAY_CONTAINS, tagsRef, array);

        // array elements keep Milvus string escaping inside the [...] constructor
        assertEquals("array_contains(tags, [\"a\", \"b\\\"\"])",
                FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), arrayFieldSchemas));
    }

    @Test
    public void testGetMilvusFilterSqlString_NullLiteral() {
        // null literals must render without NPE (previously crashed on getValue().toString())
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode nullLiteral = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, nullLiteral);

        assertEquals("name == null",
                FilterUtils.getMilvusFilterSqlString(Collections.singletonList(filter), milvusFieldSchemas));
    }

    /** Stand-in for the sqlrec-udf array_contains function operator. */
    private static final SqlOperator ARRAY_CONTAINS = new SqlFunction(
            "ARRAY_CONTAINS",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);

    /** Stand-in for the sqlrec-udf array_contains_all function operator. */
    private static final SqlOperator ARRAY_CONTAINS_ALL = new SqlFunction(
            "ARRAY_CONTAINS_ALL",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);
}
