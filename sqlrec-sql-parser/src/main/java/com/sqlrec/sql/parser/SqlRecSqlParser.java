package com.sqlrec.sql.parser;

import com.sqlrec.sql.parser.impl.SqlRecParserImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
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
        if (sqlRec.succeeded() && sqlRec.node() instanceof SqlRecStatement) {
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
            SqlNode node = SqlParser.create(sql, config.withParserFactory(factory)).parseQuery();
            return ParseAttempt.success(node);
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
