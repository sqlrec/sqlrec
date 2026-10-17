package com.sqlrec.sql.parser;

import com.sqlrec.sql.parser.impl.SqlRecParserImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.dql.SqlShowTables;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import java.util.Objects;

/** Public parser facade that keeps Flink SQL and SQLRec extensions isolated. */
public final class SqlRecSqlParser {
    private SqlRecSqlParser() {
    }

    public static SqlNode parse(String sql) throws SqlParseException {
        return parse(sql, defaultConfig());
    }

    public static SqlNode parse(String sql, SqlParser.Config config) throws SqlParseException {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(config, "config");

        ParseAttempt flink = tryParse(sql, config, FlinkSqlParserImpl.FACTORY);
        if (flink.succeeded() && flink.node().getKind() != SqlKind.PROCEDURE_CALL) {
            return flink.node();
        }

        ParseAttempt sqlRec = tryParse(sql, config, SqlRecParserImpl.FACTORY);
        if (sqlRec.succeeded() && isExtensionOrCompatibilityStatement(sqlRec.node())) {
            return sqlRec.node();
        }
        if (flink.succeeded()) {
            return flink.node();
        }
        if (sqlRec.succeeded()) {
            throw flink.error();
        }
        throw furthestError(flink.error(), sqlRec.error());
    }

    private static boolean isExtensionOrCompatibilityStatement(SqlNode node) {
        // These standard Flink nodes are also emitted by our narrow compatibility
        // productions. Other Calcite statements must not bypass the Flink parser.
        return node instanceof SqlRecStatement || node instanceof SqlSet
                || node instanceof SqlUseDatabase || node instanceof SqlShowTables;
    }

    public static SqlParser.Config defaultConfig() {
        return SqlParser.config()
                .withConformance(FlinkSqlConformance.DEFAULT)
                .withLex(Lex.JAVA);
    }

    private static ParseAttempt tryParse(
            String sql,
            SqlParser.Config config,
            SqlParserImplFactory factory) {
        try {
            SqlParser parser = SqlParser.create(sql, config.withParserFactory(factory));
            if (factory == FlinkSqlParserImpl.FACTORY) {
                return ParseAttempt.success(parser.parseQuery());
            }
            // Calcite's statement-list entry accepts a trailing delimiter without
            // changing the input text or its positions. Never return multiple SQLs.
            SqlNodeList statements = parser.parseStmtList();
            if (statements.size() != 1) {
                SqlParserPos pos = statements.size() > 1
                        ? statements.get(1).getParserPosition() : SqlParserPos.ZERO;
                throw new SqlParseException("Expected exactly one SQL statement", pos, null, null, null);
            }
            return ParseAttempt.success(statements.get(0));
        } catch (SqlParseException error) {
            return ParseAttempt.failure(error);
        }
    }

    private static SqlParseException furthestError(
            SqlParseException flinkError,
            SqlParseException sqlRecError) {
        SqlParserPos flinkPos = flinkError.getPos();
        SqlParserPos sqlRecPos = sqlRecError.getPos();
        if (sqlRecPos.getLineNum() > flinkPos.getLineNum()
                || (sqlRecPos.getLineNum() == flinkPos.getLineNum()
                && sqlRecPos.getColumnNum() > flinkPos.getColumnNum())) {
            return sqlRecError;
        }
        return flinkError;
    }

    private record ParseAttempt(SqlNode node, SqlParseException error) {
        private static ParseAttempt success(SqlNode node) {
            return new ParseAttempt(Objects.requireNonNull(node, "node"), null);
        }

        private static ParseAttempt failure(SqlParseException error) {
            return new ParseAttempt(null, Objects.requireNonNull(error, "error"));
        }

        private boolean succeeded() {
            return node != null;
        }
    }
}
