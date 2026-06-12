package com.sqlrec.connectors.mongodb.handler;

import com.sqlrec.common.schema.FieldSchema;
import com.sqlrec.connectors.mongodb.config.MongoConfig;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MongoHandlerBuildQueryTest {

    private RexBuilder rexBuilder;
    private RelDataTypeFactory typeFactory;
    private MongoConfig mongoConfig;

    @BeforeEach
    void setUp() {
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);

        mongoConfig = new MongoConfig();
        mongoConfig.uri = "mongodb://localhost:27017";
        mongoConfig.database = "testdb";
        mongoConfig.collection = "users";
        mongoConfig.primaryKey = "id";
        mongoConfig.primaryKeyIndex = 0;
        mongoConfig.fieldSchemas = Arrays.asList(
                new FieldSchema("id", "INTEGER"),
                new FieldSchema("name", "VARCHAR"),
                new FieldSchema("age", "INTEGER")
        );
    }

    private Bson invokeBuildQuery(List<RexNode> filters) throws Exception {
        MongoHandler handler = new MongoHandler(mongoConfig);
        Method method = MongoHandler.class.getDeclaredMethod("buildQuery", List.class);
        method.setAccessible(true);
        return (Bson) method.invoke(handler, filters);
    }

    private BsonDocument toBsonDocument(Bson bson) {
        return bson.toBsonDocument(BsonDocument.class, com.mongodb.MongoClientSettings.getDefaultCodecRegistry());
    }

    private String toJson(Bson bson) {
        return toBsonDocument(bson).toJson();
    }

    // --- Null / Empty ---

    @Test
    void testNullFilters() throws Exception {
        Bson result = invokeBuildQuery(null);
        assertEquals(new BsonDocument(), toBsonDocument(result));
    }

    @Test
    void testEmptyFilters() throws Exception {
        Bson result = invokeBuildQuery(Collections.emptyList());
        assertEquals(new BsonDocument(), toBsonDocument(result));
    }

    // --- EQUALS ---

    @Test
    void testEqualsInteger() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, literal);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"id\": 1}", json);
    }

    @Test
    void testEqualsString() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal = rexBuilder.makeLiteral("Alice", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, literal);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"name\": \"Alice\"}", json);
    }

    // --- NOT_EQUALS ---

    @Test
    void testNotEquals() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, ref, literal);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"id\": {\"$ne\": 1}}", json);
    }

    // --- GREATER_THAN ---

    @Test
    void testGreaterThan() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(25));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref, literal);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"age\": {\"$gt\": 25}}", json);
    }

    // --- LESS_THAN ---

    @Test
    void testLessThan() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(30));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"age\": {\"$lt\": 30}}", json);
    }

    // --- GREATER_THAN_OR_EQUAL ---

    @Test
    void testGreaterThanOrEqual() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(25));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ref, literal);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"age\": {\"$gte\": 25}}", json);
    }

    // --- LESS_THAN_OR_EQUAL ---

    @Test
    void testLessThanOrEqual() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(60));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, ref, literal);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"age\": {\"$lte\": 60}}", json);
    }

    // --- IS_NULL ---

    @Test
    void testIsNull() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ref);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"name\": null}", json);
    }

    // --- IS_NOT_NULL ---

    @Test
    void testIsNotNull() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ref);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"name\": {\"$ne\": null}}", json);
    }

    // --- AND ---

    @Test
    void testAndCondition() throws Exception {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeExactLiteral(new BigDecimal(2));
        RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal1);

        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal2 = rexBuilder.makeExactLiteral(new BigDecimal(25));
        RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref2, literal2);

        RexNode andFilter = rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2);

        Bson result = invokeBuildQuery(Collections.singletonList(andFilter));
        String json = toJson(result);

        assertEquals("{\"$and\": [{\"id\": 2}, {\"age\": {\"$gt\": 25}}]}", json);
    }

    // --- OR ---

    @Test
    void testOrCondition() throws Exception {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeExactLiteral(new BigDecimal(1));
        RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal1);

        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal2 = rexBuilder.makeLiteral("Alice", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal2);

        RexNode orFilter = rexBuilder.makeCall(SqlStdOperatorTable.OR, cond1, cond2);

        Bson result = invokeBuildQuery(Collections.singletonList(orFilter));
        String json = toJson(result);

        assertEquals("{\"$or\": [{\"id\": 1}, {\"name\": \"Alice\"}]}", json);
    }

    // --- Multiple filters (implicit AND) ---

    @Test
    void testMultipleFilters() throws Exception {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeExactLiteral(new BigDecimal(1));
        RexNode filter1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal1);

        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal2 = rexBuilder.makeExactLiteral(new BigDecimal(30));
        RexNode filter2 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref2, literal2);

        Bson result = invokeBuildQuery(Arrays.asList(filter1, filter2));
        String json = toJson(result);

        assertEquals("{\"$and\": [{\"id\": 1}, {\"age\": {\"$lt\": 30}}]}", json);
    }

    // --- Reversed operands: literal on left, field on right ---

    @Test
    void testReversedEquals() throws Exception {
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(1));
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, literal, ref);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"id\": 1}", json);
    }

    @Test
    void testReversedGreaterThan() throws Exception {
        // 25 > age  =>  age < 25
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(25));
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, literal, ref);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"age\": {\"$lt\": 25}}", json);
    }

    @Test
    void testReversedLessThan() throws Exception {
        // 30 < age  =>  age > 30
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(30));
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, literal, ref);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        assertEquals("{\"age\": {\"$gt\": 30}}", json);
    }

    // --- Complex nested AND/OR ---

    @Test
    void testComplexNestedAndOr() throws Exception {
        RexInputRef ref0 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal1 = rexBuilder.makeExactLiteral(new BigDecimal(1));
        RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref0, literal1);

        RexInputRef ref2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
        RexNode literal2 = rexBuilder.makeExactLiteral(new BigDecimal(25));
        RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ref2, literal2);

        RexNode andCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2);

        RexInputRef ref1 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
        RexNode literal3 = rexBuilder.makeLiteral("Alice", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
        RexNode cond3 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref1, literal3);

        RexNode orCond = rexBuilder.makeCall(SqlStdOperatorTable.OR, andCond, cond3);

        Bson result = invokeBuildQuery(Collections.singletonList(orCond));
        String json = toJson(result);

        assertEquals("{\"$or\": [{\"$and\": [{\"id\": 1}, {\"age\": {\"$gt\": 25}}]}, {\"name\": \"Alice\"}]}", json);
    }

    // --- Single filter should not wrap in $and ---

    @Test
    void testSingleFilterNoAndWrapper() throws Exception {
        RexInputRef ref = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode literal = rexBuilder.makeExactLiteral(new BigDecimal(1));
        RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, literal);

        Bson result = invokeBuildQuery(Collections.singletonList(filter));
        String json = toJson(result);

        // Single simple filter should produce {id: 1}, not {$and: [{id: 1}]}
        assertEquals("{\"id\": 1}", json);
    }
}
