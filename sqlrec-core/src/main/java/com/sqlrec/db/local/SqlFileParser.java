package com.sqlrec.db.local;

import com.sqlrec.compiler.CompileManager;
import com.sqlrec.sql.parser.*;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class SqlFileParser {
    private static final Logger log = LoggerFactory.getLogger(SqlFileParser.class);

    private final String sqlDirectory;

    private final List<SqlNode> tableNodes = new ArrayList<>();
    private final List<SqlNode> udfFunctionNodes = new ArrayList<>();
    private final List<List<SqlNode>> sqlFunctionNodeGroups = new ArrayList<>();
    private final List<SqlNode> apiNodes = new ArrayList<>();
    private final List<SqlNode> modelNodes = new ArrayList<>();
    private final List<SqlNode> serviceNodes = new ArrayList<>();

    public SqlFileParser(String sqlDirectory) {
        this.sqlDirectory = sqlDirectory;
    }

    public synchronized void load() {
        Path dir = Paths.get(sqlDirectory);
        if (!Files.isDirectory(dir)) {
            throw new RuntimeException("SQL schema directory does not exist: " + sqlDirectory);
        }
        try {
            List<Path> sqlFiles = collectSqlFiles(dir);
            log.info("Found {} SQL files in {}", sqlFiles.size(), sqlDirectory);
            for (Path sqlFile : sqlFiles) {
                parseSqlFile(sqlFile);
            }
            log.info("Parsed SQL files: {} tables, {} udf functions, {} sql functions, {} apis, {} models, {} services",
                    tableNodes.size(), udfFunctionNodes.size(), sqlFunctionNodeGroups.size(),
                    apiNodes.size(), modelNodes.size(), serviceNodes.size());
        } catch (Exception e) {
            throw new RuntimeException("Error loading SQL files from " + sqlDirectory, e);
        }
    }

    private List<Path> collectSqlFiles(Path dir) throws IOException {
        List<Path> sqlFiles = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.filter(Files::isRegularFile)
                    .filter(this::isSqlFile)
                    .forEach(sqlFiles::add);
        }
        return sqlFiles;
    }

    private void parseSqlFile(Path sqlFile) throws Exception {
        String fileName = sqlFile.toString();
        String content;
        try {
            content = Files.readString(sqlFile);
            parseContent(content);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing SQL file: " + fileName, e);
        }
    }

    void parseContent(String content) throws Exception {
        List<String> statements = splitStatements(content);
        log.debug("Parsing {} statements", statements.size());
        parseStatements(statements);
    }

    void parseStatements(List<String> statements) throws Exception {
        List<SqlNode> currentFunctionNodes = null;

        for (String statement : statements) {
            statement = statement.trim();
            if (statement.isEmpty()) continue;
            SqlNode sqlNode = CompileManager.parseFlinkSql(statement);
            if (currentFunctionNodes != null) {
                currentFunctionNodes.add(sqlNode);
                if (sqlNode instanceof SqlReturn) {
                    sqlFunctionNodeGroups.add(currentFunctionNodes);
                    currentFunctionNodes = null;
                }
            } else if (sqlNode instanceof SqlCreateSqlFunction) {
                currentFunctionNodes = new ArrayList<>();
                currentFunctionNodes.add(sqlNode);
            } else {
                categorizeNode(sqlNode, statement);
            }
        }

        if (currentFunctionNodes != null) {
            throw new RuntimeException("SQL function not terminated with RETURN statement");
        }
    }

    private void categorizeNode(SqlNode sqlNode, String statement) {
        if (sqlNode instanceof SqlCreateTable) {
            tableNodes.add(sqlNode);
        } else if (sqlNode instanceof SqlCreateFunction) {
            udfFunctionNodes.add(sqlNode);
        } else if (sqlNode instanceof SqlCreateApi) {
            apiNodes.add(sqlNode);
        } else if (sqlNode instanceof SqlCreateModel) {
            modelNodes.add(sqlNode);
        } else if (sqlNode instanceof SqlCreateService) {
            serviceNodes.add(sqlNode);
        } else {
            log.warn("unsupported SQL node type: {} in statement: {}", sqlNode.getClass().getSimpleName(), statement);
        }
    }

    private List<String> splitStatements(String content) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                current.append(c);
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                current.append(c);
            } else if (c == ';' && !inSingleQuote && !inDoubleQuote) {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) {
                    statements.add(stmt);
                }
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        String lastStmt = current.toString().trim();
        if (!lastStmt.isEmpty()) {
            statements.add(lastStmt);
        }

        return statements;
    }

    private boolean isSqlFile(Path path) {
        if (!Files.isRegularFile(path)) return false;
        String name = path.getFileName().toString().toLowerCase();
        return name.endsWith(".sql");
    }

    public List<SqlNode> getTableNodes() {
        return Collections.unmodifiableList(tableNodes);
    }

    public List<SqlNode> getUdfFunctionNodes() {
        return Collections.unmodifiableList(udfFunctionNodes);
    }

    public List<List<SqlNode>> getSqlFunctionNodeGroups() {
        return Collections.unmodifiableList(sqlFunctionNodeGroups);
    }

    public List<SqlNode> getApiNodes() {
        return Collections.unmodifiableList(apiNodes);
    }

    public List<SqlNode> getModelNodes() {
        return Collections.unmodifiableList(modelNodes);
    }

    public List<SqlNode> getServiceNodes() {
        return Collections.unmodifiableList(serviceNodes);
    }
}
