package com.sqlrec.connectors.jdbc.sql;

import com.sqlrec.common.schema.FieldSchema;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The single place where SQL text is assembled. Every public method returns a
 * {@link JdbcStatement}, whose SQL text contains only dialect-quoted identifiers
 * (see {@link #quoteIdentifier(String, String)}) and {@code '?'} placeholders; literal
 * values always travel as bind parameters. {@link JdbcStatement} can only be constructed
 * inside this package, so code outside it cannot produce SQL with inlined values.
 */
public final class JdbcSqlBuilder {
    // Dialect reserved words, not Calcite's SQL-standard list: quoting otherwise legal
    // names such as DATE changes case folding and can break existing columns.
    // H2 2.2 ParserUtil keywords; PostgreSQL 17 kwlist.h RESERVED_KEYWORD;
    // MySQL 8.0 Reference Manual, Keywords and Reserved Words (entries marked R).
    private static final Set<String> H2_RESERVED = keywords(
            "ALL AND ANY ARRAY AS ASYMMETRIC AUTHORIZATION BETWEEN CASE CAST CHECK CONSTRAINT CROSS"
            + " CURRENT_CATALOG CURRENT_DATE CURRENT_PATH CURRENT_ROLE CURRENT_SCHEMA CURRENT_TIME"
            + " CURRENT_TIMESTAMP CURRENT_USER DAY DEFAULT DISTINCT ELSE END EXCEPT EXISTS FALSE FETCH FOR"
            + " FOREIGN FROM FULL GROUP HAVING HOUR IF IN INNER INTERSECT INTERVAL IS JOIN KEY LEFT LIKE LIMIT"
            + " LOCALTIME LOCALTIMESTAMP MINUS MINUTE MONTH NATURAL NOT NULL OFFSET ON OR ORDER PRIMARY QUALIFY"
            + " RIGHT ROW ROWNUM SECOND SELECT SESSION_USER SET SOME SYMMETRIC SYSTEM_USER TABLE TO TRUE UESCAPE"
            + " UNION UNIQUE UNKNOWN USER USING VALUE VALUES WHEN WHERE WINDOW WITH YEAR _ROWID_");
    private static final Set<String> POSTGRES_RESERVED = keywords(
            "ALL ANALYSE ANALYZE AND ANY ARRAY AS ASC ASYMMETRIC BOTH CASE CAST CHECK COLLATE COLUMN"
            + " CONSTRAINT CREATE CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_TIME CURRENT_TIMESTAMP"
            + " CURRENT_USER DEFAULT DEFERRABLE DESC DISTINCT DO ELSE END EXCEPT FALSE FETCH FOR FOREIGN FROM"
            + " GRANT GROUP HAVING IN INITIALLY INTERSECT INTO LATERAL LEADING LIMIT LOCALTIME LOCALTIMESTAMP"
            + " NOT NULL OFFSET ON ONLY OR ORDER PLACING PRIMARY REFERENCES RETURNING SELECT SESSION_USER SOME"
            + " SYMMETRIC SYSTEM_USER TABLE THEN TO TRAILING TRUE UNION UNIQUE USER USING VARIADIC WHEN WHERE"
            + " WINDOW WITH");
    private static final Set<String> MYSQL_RESERVED = keywords(
            "ACCESSIBLE ADD ALL ALTER ANALYZE AND AS ASC ASENSITIVE BEFORE BETWEEN BIGINT BINARY BLOB BOTH BY"
            + " CALL CASCADE CASE CHANGE CHAR CHARACTER CHECK COLLATE COLUMN CONDITION CONSTRAINT CONTINUE"
            + " CONVERT CREATE CROSS CUBE CUME_DIST CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER"
            + " CURSOR DATABASE DATABASES DAY_HOUR DAY_MICROSECOND DAY_MINUTE DAY_SECOND DEC DECIMAL DECLARE"
            + " DEFAULT DELAYED DELETE DENSE_RANK DESC DESCRIBE DETERMINISTIC DISTINCT DISTINCTROW DIV DOUBLE"
            + " DROP DUAL EACH ELSE ELSEIF EMPTY ENCLOSED ESCAPED EXCEPT EXISTS EXIT EXPLAIN FALSE FETCH"
            + " FIRST_VALUE FLOAT FLOAT4 FLOAT8 FOR FORCE FOREIGN FROM FULLTEXT FUNCTION GENERATED GET GRANT"
            + " GROUP GROUPING GROUPS HAVING HIGH_PRIORITY HOUR_MICROSECOND HOUR_MINUTE HOUR_SECOND IF IGNORE IN"
            + " INDEX INFILE INNER INOUT INSENSITIVE INSERT INT INT1 INT2 INT3 INT4 INT8 INTEGER INTERSECT"
            + " INTERVAL INTO IO_AFTER_GTIDS IO_BEFORE_GTIDS IS ITERATE JOIN JSON_TABLE KEY KEYS KILL LAG"
            + " LAST_VALUE LATERAL LEAD LEADING LEAVE LEFT LIKE LIMIT LINEAR LINES LOAD LOCALTIME LOCALTIMESTAMP"
            + " LOCK LONG LONGBLOB LONGTEXT LOOP LOW_PRIORITY MASTER_BIND MASTER_SSL_VERIFY_SERVER_CERT MATCH"
            + " MAXVALUE MEDIUMBLOB MEDIUMINT MEDIUMTEXT MIDDLEINT MINUTE_MICROSECOND MINUTE_SECOND MOD MODIFIES"
            + " NATURAL NOT NO_WRITE_TO_BINLOG NTH_VALUE NTILE NULL NUMERIC OF ON OPTIMIZE OPTIMIZER_COSTS"
            + " OPTION OPTIONALLY OR ORDER OUT OUTER OUTFILE OVER PARTITION PERCENT_RANK PRECISION PRIMARY"
            + " PROCEDURE PURGE RANGE RANK READ READS READ_WRITE REAL RECURSIVE REFERENCES REGEXP RELEASE RENAME"
            + " REPEAT REPLACE REQUIRE RESIGNAL RESTRICT RETURN REVOKE RIGHT RLIKE ROW ROWS ROW_NUMBER SCHEMA"
            + " SCHEMAS SECOND_MICROSECOND SELECT SENSITIVE SEPARATOR SET SHOW SIGNAL SMALLINT SPATIAL SPECIFIC"
            + " SQL SQLEXCEPTION SQLSTATE SQLWARNING SQL_BIG_RESULT SQL_CALC_FOUND_ROWS SQL_SMALL_RESULT SSL"
            + " STARTING STORED STRAIGHT_JOIN SYSTEM TABLE TERMINATED THEN TINYBLOB TINYINT TINYTEXT TO TRAILING"
            + " TRIGGER TRUE UNDO UNION UNIQUE UNLOCK UNSIGNED UPDATE USAGE USE USING UTC_DATE UTC_TIME"
            + " UTC_TIMESTAMP VALUES VARBINARY VARCHAR VARCHARACTER VARYING VIRTUAL WHEN WHERE WHILE WINDOW WITH"
            + " WRITE XOR YEAR_MONTH ZEROFILL _FILENAME");

    private JdbcSqlBuilder() {
    }

    /**
     * Build a SELECT for the whole table, optionally with a WHERE clause translated from
     * Calcite filter conditions (see {@link JdbcFilterBuilder#build}).
     */
    public static JdbcStatement select(String url, String tableName, List<FieldSchema> fieldSchemas, List<RexNode> filters) {
        validateFieldSchemas(fieldSchemas);
        JdbcStatement where = JdbcFilterBuilder.build(filters, fieldSchemas, url);
        StringBuilder sql = new StringBuilder("SELECT ");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(" FROM ").append(quoteQualifiedIdentifier(tableName, url));
        if (!where.getSql().isEmpty()) {
            sql.append(" WHERE ").append(where.getSql());
        }
        return new JdbcStatement(sql.toString(), where.getParameters());
    }

    /**
     * Build a SELECT of rows whose primary key is in the given key set.
     * The returned statement carries {@code keyCount} unbound placeholders; the caller
     * must supply the key values via {@link JdbcStatement#withParameters(List)}.
     */
    public static JdbcStatement selectByPrimaryKey(String url, String tableName, List<FieldSchema> fieldSchemas,
                                                  String primaryKey, int keyCount) {
        validateFieldSchemas(fieldSchemas);
        if (keyCount <= 0) {
            throw new IllegalArgumentException("keyCount must be positive: " + keyCount);
        }
        StringBuilder sql = new StringBuilder("SELECT ");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(" FROM ").append(quoteQualifiedIdentifier(tableName, url));
        sql.append(" WHERE ").append(quoteIdentifier(primaryKey, url)).append(" IN (");
        appendPlaceholders(sql, keyCount);
        sql.append(")");
        return new JdbcStatement(sql.toString(), Collections.emptyList());
    }

    /**
     * Build an upsert statement (INSERT ... ON CONFLICT/ON DUPLICATE KEY or MERGE INTO,
     * depending on the dialect inferred from the JDBC url). The returned statement is a
     * row template: the caller supplies one value per column via
     * {@link JdbcStatement#withParameters(List)}.
     */
    public static JdbcStatement upsert(String url, String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        validateFieldSchemas(fieldSchemas);
        String lowerUrl = url.toLowerCase();
        if (lowerUrl.startsWith("jdbc:mysql:")) {
            return mysqlUpsert(tableName, fieldSchemas, primaryKey);
        }
        if (lowerUrl.startsWith("jdbc:h2:")) {
            return h2Upsert(tableName, fieldSchemas, primaryKey);
        }
        // PostgreSQL
        return postgresUpsert(tableName, fieldSchemas, primaryKey);
    }

    /** PostgreSQL uses ANSI double-quoted identifiers. */
    private static JdbcStatement postgresUpsert(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        String url = null;
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(quoteQualifiedIdentifier(tableName, url)).append(" (");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(") ON CONFLICT (").append(quoteIdentifier(primaryKey, url)).append(") DO UPDATE SET ");
        appendExcludedSet(sql, fieldSchemas, url);
        return new JdbcStatement(sql.toString(), Collections.emptyList());
    }

    /** MySQL uses backtick-quoted identifiers. */
    private static JdbcStatement mysqlUpsert(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        String url = "jdbc:mysql:";
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(quoteQualifiedIdentifier(tableName, url)).append(" (");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(") ON DUPLICATE KEY UPDATE ");
        appendValuesSet(sql, fieldSchemas, url);
        return new JdbcStatement(sql.toString(), Collections.emptyList());
    }

    /** H2 uses ANSI double-quoted identifiers. */
    private static JdbcStatement h2Upsert(String tableName, List<FieldSchema> fieldSchemas, String primaryKey) {
        String url = "jdbc:h2:";
        StringBuilder sql = new StringBuilder("MERGE INTO ");
        sql.append(quoteQualifiedIdentifier(tableName, url)).append(" (");
        appendColumnNames(sql, fieldSchemas, url);
        sql.append(") KEY (");
        sql.append(quoteIdentifier(primaryKey, url)).append(") VALUES (");
        appendPlaceholders(sql, fieldSchemas.size());
        sql.append(")");
        return new JdbcStatement(sql.toString(), Collections.emptyList());
    }

    /**
     * Build a delete-by-primary-key statement. The returned statement is a row template:
     * the caller supplies the key value via {@link JdbcStatement#withParameters(List)}.
     */
    public static JdbcStatement deleteByPrimaryKey(String url, String tableName, String primaryKey) {
        String sql = "DELETE FROM " + quoteQualifiedIdentifier(tableName, url)
                + " WHERE " + quoteIdentifier(primaryKey, url) + " = ?";
        return new JdbcStatement(sql, Collections.emptyList());
    }

    private static void appendColumnNames(StringBuilder sql, List<FieldSchema> fieldSchemas, String url) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(fieldSchemas.get(i).getName(), url));
        }
    }

    private static void appendPlaceholders(StringBuilder sql, int count) {
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
    }

    private static void appendExcludedSet(StringBuilder sql, List<FieldSchema> fieldSchemas, String url) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String colName = quoteIdentifier(fieldSchemas.get(i).getName(), url);
            sql.append(colName).append(" = EXCLUDED.").append(colName);
        }
    }

    private static void appendValuesSet(StringBuilder sql, List<FieldSchema> fieldSchemas, String url) {
        for (int i = 0; i < fieldSchemas.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String colName = quoteIdentifier(fieldSchemas.get(i).getName(), url);
            sql.append(colName).append(" = VALUES(").append(colName).append(")");
        }
    }

    /**
     * Quote a SQL identifier (table/column name) according to the dialect inferred from the JDBC url.
     * <p>
     * This is the only place raw identifiers may enter SQL text. Non-keyword safe identifiers (matching
     * {@code [a-zA-Z_][a-zA-Z0-9_]*}) are returned as-is to preserve the database's default
     * case-folding behavior and avoid breaking existing schemas. Unsafe identifiers (containing
     * special characters, spaces, quotes, etc.) are quoted to prevent SQL injection and syntax
     * errors. MySQL uses backticks; PostgreSQL/H2 and the default use ANSI double quotes. The
     * corresponding quote character is escaped by doubling.
     */
    public static String quoteIdentifier(String identifier, String url) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        if (isSafeIdentifier(identifier) && !reservedWords(url).contains(identifier.toUpperCase(Locale.ROOT))) {
            return identifier;
        }
        boolean mySql = url != null && url.toLowerCase().startsWith("jdbc:mysql:");
        if (mySql) {
            return "`" + identifier.replace("`", "``") + "`";
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static Set<String> keywords(String words) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(words.split(" "))));
    }

    private static Set<String> reservedWords(String url) {
        String dialect = url == null ? "" : url.toLowerCase(Locale.ROOT);
        if (dialect.startsWith("jdbc:h2:")) return H2_RESERVED;
        if (dialect.startsWith("jdbc:mysql:")) return MYSQL_RESERVED;
        return POSTGRES_RESERVED;
    }

    /**
     * Quote each part of a possibly qualified table name independently.
     * Quoting the whole value would turn {@code schema.table} into one identifier
     * named {@code schema.table}, which is not the same object in SQL.
     */
    public static String quoteQualifiedIdentifier(String identifier, String url) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        String[] parts = identifier.split("\\.", -1);
        for (String part : parts) {
            if (part.isEmpty()) {
                throw new IllegalArgumentException("Invalid qualified identifier: " + identifier);
            }
        }
        StringBuilder result = new StringBuilder(identifier.length() + parts.length * 2);
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                result.append('.');
            }
            result.append(quoteIdentifier(parts[i], url));
        }
        return result.toString();
    }

    private static void validateFieldSchemas(List<FieldSchema> fieldSchemas) {
        if (fieldSchemas == null || fieldSchemas.isEmpty()) {
            throw new IllegalArgumentException("fieldSchemas must not be null or empty");
        }
    }

    /**
     * Returns true if the identifier consists only of {@code [a-zA-Z_][a-zA-Z0-9_]*} and therefore
     * does not need quoting (it cannot break out of an identifier context or inject SQL).
     */
    private static boolean isSafeIdentifier(String identifier) {
        char first = identifier.charAt(0);
        if (!Character.isLetter(first) && first != '_') {
            return false;
        }
        for (int i = 1; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                return false;
            }
        }
        return true;
    }
}
