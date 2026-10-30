package com.sqlrec.db.local;

import com.sqlrec.common.config.Consts;
import com.sqlrec.db.FlinkTableDdl;
import com.sqlrec.db.MetadataQueryUtils;
import com.sqlrec.db.SchemaAccess;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.utils.SchemaUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.*;
import org.apache.flink.sql.parser.ddl.resource.SqlResource;
import org.apache.flink.sql.parser.ddl.resource.SqlResourceType;
import org.apache.flink.sql.parser.dql.SqlRichDescribeTable;
import org.apache.flink.sql.parser.dql.SqlShowCreateTable;
import org.apache.flink.sql.parser.dql.SqlShowFunctions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.ShowCreateUtil;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.hadoop.hive.metastore.api.*;

import java.util.*;

/** SQL files initialize this process-local store; runtime DDL never writes back to the files. */
public class SqlFileSchemaAccess implements SchemaAccess {
    private final Map<String, Database> databases = names();

    private static final class Database {
        private final String name;
        private final String comment;
        private final Map<String, String> properties;
        private final Map<String, TableEntry> tables = names();
        private final Map<String, Function> functions = names();

        private Database(String name, String comment, Map<String, String> properties) {
            this.name = name;
            this.comment = comment;
            this.properties = new HashMap<>(properties);
        }
    }

    private record TableEntry(ResolvedCatalogTable definition, Table metadata) {}

    public SqlFileSchemaAccess(List<SqlNode> tableNodes, List<SqlNode> udfFunctionNodes) throws Exception {
        loadedDatabase(Consts.DEFAULT_SCHEMA_NAME);
        for (SqlNode node : tableNodes) {
            if (!(node instanceof SqlCreateTable create)) throw unsupported(node);
            ObjectPath path = MetadataQueryUtils.objectPath(create.fullTableName(), Consts.DEFAULT_SCHEMA_NAME);
            loadedDatabase(path.getDatabaseName());
            createTable(create, Consts.DEFAULT_SCHEMA_NAME);
        }
        for (SqlNode node : udfFunctionNodes) {
            if (!(node instanceof SqlCreateFunction create)) throw unsupported(node);
            ObjectPath path = MetadataQueryUtils.objectPath(create.getFunctionIdentifier(), Consts.DEFAULT_SCHEMA_NAME);
            loadedDatabase(path.getDatabaseName());
            createFunction(create, Consts.DEFAULT_SCHEMA_NAME);
        }
    }

    private static <T> Map<String, T> names() {
        return new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    private Database loadedDatabase(String name) {
        return databases.computeIfAbsent(name, key -> new Database(key, null, Map.of()));
    }

    @Override
    public synchronized void executeMetadataDdl(SqlNode node, String database) throws Exception {
        switch (node) {
            case SqlCreateDatabase create -> createDatabase(create, database);
            case SqlAlterDatabase alter -> alterDatabase(alter, database);
            case SqlDropDatabase drop -> dropDatabase(drop, database);
            case SqlCreateTable create -> createTable(create, database);
            case SqlAlterTable alter -> alterTable(alter, database);
            case SqlDropTable drop -> dropTable(drop, database);
            case SqlCreateFunction create -> createFunction(create, database);
            case SqlAlterFunction alter -> alterFunction(alter, database);
            case SqlDropFunction drop -> dropFunction(drop, database);
            default -> throw unsupported(node);
        }
    }

    private void createDatabase(SqlCreateDatabase sql, String current) throws Exception {
        String name = MetadataQueryUtils.databaseName(sql.fullDatabaseName(), current);
        if (databases.containsKey(name)) {
            if (sql.isIfNotExists()) return;
            throw new AlreadyExistsException("Database already exists: " + name);
        }
        Database db = new Database(name, sql.getComment().map(SchemaUtils::getValueOfStringLiteral).orElse(null),
                FlinkTableDdl.properties(sql.getPropertyList()));
        databases.put(name, db);
    }

    private void alterDatabase(SqlAlterDatabase sql, String current) throws Exception {
        Database db = requireDatabase(MetadataQueryUtils.databaseName(sql.fullDatabaseName(), current));
        Map<String, String> properties = FlinkTableDdl.properties(sql.getPropertyList());
        db.properties.putAll(properties);
    }

    private void dropDatabase(SqlDropDatabase sql, String current) throws Exception {
        String name = MetadataQueryUtils.databaseName(sql.fullDatabaseName(), current);
        Database db = databases.get(name);
        if (db == null) {
            if (sql.getIfExists()) return;
            throw new NoSuchObjectException("Database does not exist: " + name);
        }
        if (name.equalsIgnoreCase(current)) {
            throw new ValidationException("Cannot drop the current database; USE another database first: " + name);
        }
        if (name.equalsIgnoreCase(Consts.DEFAULT_SCHEMA_NAME)) {
            throw new ValidationException("Cannot drop the default database");
        }
        if (!sql.isCascade() && (!db.tables.isEmpty() || !db.functions.isEmpty())) {
            throw new ValidationException("Database is not empty: " + name);
        }
        databases.remove(name);
    }

    private void createTable(SqlCreateTable sql, String current) throws Exception {
        if (sql.isTemporary() || sql instanceof SqlCreateTableAs) throw unsupported(sql);
        ObjectPath path = MetadataQueryUtils.objectPath(sql.fullTableName(), current);
        Database db = requireDatabase(path.getDatabaseName());
        if (db.tables.containsKey(path.getObjectName())) {
            if (sql.isIfNotExists()) return;
            throw new AlreadyExistsException("Table already exists: " + path.getFullName());
        }
        ResolvedCatalogTable definition;
        if (sql instanceof SqlCreateTableLike like) {
            ObjectPath source = MetadataQueryUtils.objectPath(
                    like.getTableLike().getSourceTable().names.toArray(String[]::new), current);
            definition = FlinkTableDdl.createLike(like, tableDefinition(source).definition());
        } else {
            definition = FlinkTableDdl.create(sql);
        }
        TableEntry table = tableEntry(new ObjectPath(db.name, path.getObjectName()), definition);
        db.tables.put(path.getObjectName(), table);
    }

    private void alterTable(SqlAlterTable sql, String current) throws Exception {
        if (sql instanceof SqlAddPartitions || sql instanceof SqlDropPartitions || sql.getPartitionSpec() != null) {
            throw unsupported(sql);
        }
        ObjectPath path = MetadataQueryUtils.objectPath(sql.fullTableName(), current);
        Database db = requireDatabase(path.getDatabaseName());
        TableEntry previous = db.tables.get(path.getObjectName());
        if (previous == null) {
            if (sql.ifTableExists()) return;
            throw new NoSuchObjectException("Table does not exist: " + path.getFullName());
        }
        if (sql instanceof SqlAlterTableRename rename) {
            ObjectPath target = MetadataQueryUtils.objectPath(rename.fullNewTableName(), db.name);
            if (!db.name.equalsIgnoreCase(target.getDatabaseName())) {
                throw new UnsupportedOperationException("Renaming a table across databases is not supported");
            }
            if (db.tables.containsKey(target.getObjectName())) {
                throw new AlreadyExistsException("Table already exists: " + target.getFullName());
            }
            TableEntry replacement = tableEntry(new ObjectPath(db.name, target.getObjectName()), previous.definition());
            db.tables.remove(path.getObjectName());
            db.tables.put(target.getObjectName(), replacement);
        } else {
            TableEntry replacement = tableEntry(new ObjectPath(db.name, previous.metadata().getTableName()),
                    FlinkTableDdl.alter(sql, previous.definition()));
            db.tables.put(path.getObjectName(), replacement);
        }
    }

    private void dropTable(SqlDropTable sql, String current) throws Exception {
        if (sql.isTemporary()) throw unsupported(sql);
        ObjectPath path = MetadataQueryUtils.objectPath(sql.fullTableName(), current);
        Database db = requireDatabase(path.getDatabaseName());
        if (db.tables.remove(path.getObjectName()) == null && !sql.getIfExists()) {
            throw new NoSuchObjectException("Table does not exist: " + path.getFullName());
        }
    }

    private void createFunction(SqlCreateFunction sql, String current) throws Exception {
        if (sql.isTemporary() || sql.isSystemFunction()) throw unsupported(sql);
        requireJava(sql.getFunctionLanguage());
        ObjectPath path = MetadataQueryUtils.objectPath(sql.getFunctionIdentifier(), current);
        Database db = requireDatabase(path.getDatabaseName());
        if (db.functions.containsKey(path.getObjectName())) {
            if (sql.isIfNotExists()) return;
            throw new AlreadyExistsException("Function already exists: " + path.getFullName());
        }
        Function definition = function(new ObjectPath(db.name, path.getObjectName()),
                SchemaUtils.getValueOfStringLiteral(sql.getFunctionClassName()), sql.getResourceInfos());
        db.functions.put(path.getObjectName(), definition);
    }

    private void alterFunction(SqlAlterFunction sql, String current) throws Exception {
        if (sql.isTemporary() || sql.isSystemFunction()) throw unsupported(sql);
        requireJava(sql.getFunctionLanguage());
        ObjectPath path = MetadataQueryUtils.objectPath(sql.getFunctionIdentifier(), current);
        Database db = requireDatabase(path.getDatabaseName());
        Function previous = db.functions.get(path.getObjectName());
        if (previous == null) {
            if (sql.isIfExists()) return;
            throw new NoSuchObjectException("Function does not exist: " + path.getFullName());
        }
        previous.setClassName(SchemaUtils.getValueOfStringLiteral(sql.getFunctionClassName()));
    }

    private void dropFunction(SqlDropFunction sql, String current) throws Exception {
        if (sql.isTemporary() || sql.isSystemFunction()) throw unsupported(sql);
        ObjectPath path = MetadataQueryUtils.objectPath(sql.getFunctionIdentifier(), current);
        Database db = requireDatabase(path.getDatabaseName());
        if (db.functions.remove(path.getObjectName()) == null && !sql.getIfExists()) {
            throw new NoSuchObjectException("Function does not exist: " + path.getFullName());
        }
    }

    private static void requireJava(String language) {
        if (language != null && !"JAVA".equalsIgnoreCase(language)) {
            throw new UnsupportedOperationException("Only Java UDF DDL is supported in SQL file metadata mode");
        }
    }

    private static Function function(ObjectPath path, String className, List<SqlNode> resources) {
        Function definition = new Function();
        definition.setDbName(path.getDatabaseName());
        definition.setFunctionName(path.getObjectName());
        definition.setClassName(className);
        definition.setFunctionType(FunctionType.JAVA);
        definition.setResourceUris(resources.stream().map(SqlResource.class::cast)
                .map(resource -> new ResourceUri(ResourceType.valueOf(
                        resource.getResourceType().getValueAs(SqlResourceType.class).name()),
                        SchemaUtils.getValueOfStringLiteral(resource.getResourcePath()))).toList());
        return definition;
    }

    private TableEntry tableEntry(ObjectPath path, ResolvedCatalogTable definition) {
        Table table = new Table();
        table.setDbName(path.getDatabaseName());
        table.setTableName(path.getObjectName());
        table.setSd(new StorageDescriptor());
        table.getSd().setCols(definition.getResolvedSchema().getColumns().stream()
                .map(column -> new FieldSchema(column.getName(), column.getDataType().getLogicalType().asSerializableString(),
                        column.getComment().orElse(null))).toList());
        Map<String, String> properties = new HashMap<>();
        definition.toProperties().forEach((key, value) -> properties.put("flink." + key, value));
        properties.put("transient_lastDdlTime", Long.toString(System.currentTimeMillis() / 1000));
        table.setParameters(properties);
        return new TableEntry(definition, table);
    }

    @Override
    public synchronized SqlProcessResult executeMetadataQuery(SqlNode node, String database) throws Exception {
        if (node instanceof SqlShowCreateTable show) {
            TableEntry table = tableDefinition(MetadataQueryUtils.objectPath(
                    show.getTableName().names.toArray(String[]::new), database));
            return SqlProcessResult.msg(ShowCreateUtil.buildShowCreateTableRow(table.definition(),
                    ObjectIdentifier.of(Consts.HIVE_CATALOG_NAME, table.metadata().getDbName(),
                            table.metadata().getTableName()), false), "result");
        }
        if (node instanceof SqlRichDescribeTable describe) {
            return MetadataQueryUtils.describeTable(tableDefinition(
                    MetadataQueryUtils.objectPath(describe.fullTableName(), database)).definition().getResolvedSchema());
        }
        if (node instanceof SqlShowFunctions show) {
            String name = MetadataQueryUtils.databaseName(show.fullDatabaseName(), database);
            requireDatabase(name);
            return MetadataQueryUtils.showFunctions(show,
                    getFunctions(name).stream().map(Function::getFunctionName).toList());
        }
        return SchemaAccess.super.executeMetadataQuery(node, database);
    }

    private TableEntry tableDefinition(ObjectPath path) throws NoSuchObjectException {
        TableEntry table = requireDatabase(path.getDatabaseName()).tables.get(path.getObjectName());
        if (table == null) throw new NoSuchObjectException("Table does not exist: " + path.getFullName());
        return table;
    }

    private Database requireDatabase(String name) throws NoSuchObjectException {
        Database db = databases.get(name);
        if (db == null) throw new NoSuchObjectException("Database does not exist: " + name);
        return db;
    }

    @Override
    public synchronized List<String> getDatabases() {
        return new ArrayList<>(databases.keySet());
    }

    @Override
    public synchronized List<Table> getTables(String database) {
        Database db = databases.get(database);
        return db == null ? List.of() : db.tables.values().stream().map(table -> table.metadata().deepCopy()).toList();
    }

    @Override
    public synchronized Table getTable(String database, String tableName) throws Exception {
        return tableDefinition(new ObjectPath(database, tableName)).metadata().deepCopy();
    }

    @Override
    public synchronized List<Function> getFunctions(String database) {
        Database db = databases.get(database);
        return db == null ? List.of() : db.functions.values().stream().map(Function::deepCopy).toList();
    }

    @Override
    public synchronized Function getFunction(String database, String name) throws Exception {
        Function function = requireDatabase(database).functions.get(name);
        if (function == null) throw new NoSuchObjectException("Function does not exist: " + database + "." + name);
        return function.deepCopy();
    }

    @Override
    public List<String> getPartitionPaths(String database, String table, String partitionFilter) {
        throw new UnsupportedOperationException("getPartitionPaths is not supported in SqlFileSchemaAccess");
    }

    private static UnsupportedOperationException unsupported(SqlNode node) {
        return new UnsupportedOperationException("UNSUPPORTED_METADATA_DDL in SQL file metadata mode: "
                + node.getClass().getSimpleName());
    }
}
