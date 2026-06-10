package com.sqlrec.common.utils;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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

    // --- SQL filter tests ---

    private List<FieldSchema> sqlFieldSchemas = Arrays.asList(
            new FieldSchema("id", "INTEGER"),
            new FieldSchema("name", "VARCHAR"),
            new FieldSchema("age", "INTEGER")
    );

    @Test
    public void testGetSqlFilterString_Empty() {
        assertEquals("", FilterUtils.getSqlFilterString(Collections.emptyList(), sqlFieldSchemas));
    }

    @Test
    public void testGetSqlFilterString_Null() {
        assertEquals("", FilterUtils.getSqlFilterString((List<RexNode>) null, sqlFieldSchemas));
    }

    @Test
    public void testGetSqlFilterString_Equals() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexLiteral literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        String result = FilterUtils.getSqlFilterString(Collections.singletonList(filter), sqlFieldSchemas);
        assertEquals("id = 1", result);
    }

    @Test
    public void testGetSqlFilterString_StringEquals() {
        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal);

        String result = FilterUtils.getSqlFilterString(Collections.singletonList(filter), sqlFieldSchemas);
        assertEquals("name = 'Bob'", result);
    }

    @Test
    public void testGetSqlFilterString_GreaterThanOrEqual() {
        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(25));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ref2, literal);

        String result = FilterUtils.getSqlFilterString(Collections.singletonList(filter), sqlFieldSchemas);
        assertEquals("age >= 25", result);
    }

    @Test
    public void testGetSqlFilterString_AndCondition() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeExactLiteral(new java.math.BigDecimal(2));
        RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal1);

        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal2 = rexBuilder.makeLiteral("Bob", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal2);

        RexNode andFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2);

        String result = FilterUtils.getSqlFilterString(Collections.singletonList(andFilter), sqlFieldSchemas);
        assertEquals("(id = 2) AND (name = 'Bob')", result);
    }

    @Test
    public void testGetSqlFilterString_MultipleFilters() {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new java.math.BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal);

        String result = FilterUtils.getSqlFilterString(Arrays.asList(filter), sqlFieldSchemas);
        assertEquals("id = 1", result);
    }
}
