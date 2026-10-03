package com.sqlrec.db.remote;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.CompileManager;
import com.sqlrec.executor.SqlProcessResult;
import com.sqlrec.db.FlinkTableDdl;
import com.sqlrec.db.MetadataQueryUtils;
import org.apache.calcite.sql.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.ddl.*;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.sql.parser.ddl.resource.SqlResource;
import org.apache.flink.sql.parser.ddl.resource.SqlResourceType;
import org.apache.flink.sql.parser.dql.*;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.ShowCreateUtil;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.thrift.transport.TTransportException;

import java.util.*;
import java.util.function.Supplier;

import static com.sqlrec.db.MetadataQueryUtils.objectPath;
import static com.sqlrec.db.MetadataQueryUtils.databaseName;

/** Flink metadata execution through the official HiveCatalog, without a planner or Gateway. */
public final class FlinkHiveDdlAdapter implements AutoCloseable {
    private final Supplier<CatalogManager> catalogFactory;
    private CatalogManager catalogs;

    public FlinkHiveDdlAdapter() {
        this(FlinkHiveDdlAdapter::createCatalogs);
    }

    /** The factory transfers ownership of an opened manager to this adapter. */
    FlinkHiveDdlAdapter(Supplier<CatalogManager> catalogFactory) {
        this.catalogFactory = Objects.requireNonNull(catalogFactory);
    }

    /** Convenience entry point for callers without an already parsed statement. */
    public void executeDdl(String sql, String database) throws Exception {
        executeDdl(parseSql(sql), database);
    }

    public synchronized void executeDdl(SqlNode node, String database) throws Exception {
        try {
            requirePersistentDdl(node);
            switch (node) {
                case SqlCreateDatabase create -> createDatabase(create, database);
                case SqlAlterDatabase alter -> alterDatabase(alter, database);
                case SqlDropDatabase drop -> dropDatabase(drop, database);
                case SqlCreateTableLike like -> createTableLike(like, database);
                case SqlCreateTable create -> createTable(create, database);
                case SqlAlterTable alter -> alterTable(alter, database);
                case SqlDropTable drop -> dropTable(drop, database);
                case SqlCreateFunction create -> createFunction(create, database);
                case SqlAlterFunction alter -> alterFunction(alter, database);
                case SqlDropFunction drop -> dropFunction(drop, database);
                default -> throw unsupported(node);
            }
        } catch (Exception e) {
            throw executionFailure(e);
        }
    }

    private void createDatabase(SqlCreateDatabase sql, String database) throws Exception {
        String name = databaseName(sql.fullDatabaseName(), database);
        var definition = new CatalogDatabaseImpl(FlinkTableDdl.properties(sql.getPropertyList()),
                sql.getComment().map(FlinkHiveDdlAdapter::literal).orElse(null));
        HiveCatalog catalog = hiveCatalog(catalogs());
        write(() -> catalog.createDatabase(name, definition, sql.isIfNotExists()));
    }

    private void alterDatabase(SqlAlterDatabase sql, String database) throws Exception {
        String name = databaseName(sql.fullDatabaseName(), database);
        HiveCatalog catalog = hiveCatalog(catalogs());
        CatalogDatabase previous = catalog.getDatabase(name);
        Map<String, String> properties = new HashMap<>(previous.getProperties());
        properties.putAll(FlinkTableDdl.properties(sql.getPropertyList()));
        var definition = new CatalogDatabaseImpl(properties, previous.getComment());
        write(() -> catalog.alterDatabase(name, definition, false));
    }

    private void dropDatabase(SqlDropDatabase sql, String database) throws Exception {
        String name = databaseName(sql.fullDatabaseName(), database);
        HiveCatalog catalog = hiveCatalog(catalogs());
        if (name.equals(database) && catalog.databaseExists(name)) {
            throw new ValidationException("Cannot drop the current database; USE another database first: " + name);
        }
        write(() -> catalog.dropDatabase(name, sql.getIfExists(), sql.isCascade()));
    }

    private void createTable(SqlCreateTable sql, String database) throws Exception {
        String[] names = sql.fullTableName();
        ObjectPath path = objectPath(names, database);
        HiveCatalog catalog = catalogForDdl(database, names.length == 1);
        ResolvedCatalogTable definition = FlinkTableDdl.create(sql, catalogs().getDataTypeFactory());
        write(() -> catalog.createTable(path, definition, sql.isIfNotExists()));
    }

    private void createTableLike(SqlCreateTableLike sql, String database) throws Exception {
        String[] names = sql.fullTableName();
        String[] sourceNames = sql.getTableLike().getSourceTable().names.toArray(String[]::new);
        ObjectPath path = objectPath(names, database);
        ObjectPath sourcePath = objectPath(sourceNames, database);
        HiveCatalog catalog = catalogForDdl(database, names.length == 1 || sourceNames.length == 1);
        DataTypeFactory types = catalogs().getDataTypeFactory();
        ResolvedCatalogTable source = FlinkTableDdl.resolve(catalog.getTable(sourcePath), types);
        ResolvedCatalogTable definition = FlinkTableDdl.createLike(sql, source, types);
        write(() -> catalog.createTable(path, definition, sql.isIfNotExists()));
    }

    private void dropTable(SqlDropTable sql, String database) throws Exception {
        String[] names = sql.fullTableName();
        ObjectPath path = objectPath(names, database);
        HiveCatalog catalog = catalogForDdl(database, names.length == 1);
        write(() -> catalog.dropTable(path, sql.getIfExists()));
    }

    private void alterTable(SqlAlterTable sql, String database) throws Exception {
        String[] names = sql.fullTableName();
        ObjectPath path = objectPath(names, database);
        HiveCatalog catalog = catalogForDdl(database, names.length == 1);
        if (sql.ifTableExists() && !catalog.tableExists(path)) return;
        switch (sql) {
            case SqlAlterTableRename rename -> renameTable(rename, path, catalog);
            case SqlAddPartitions add -> addPartitions(add, path, catalog);
            case SqlDropPartitions drop -> dropPartitions(drop, path, catalog);
            case SqlAlterTableOptions options when sql.getPartitionSpec() != null -> alterPartition(options, path, catalog);
            default -> {
                ResolvedCatalogTable definition = FlinkTableDdl.alter(sql, catalog.getTable(path), catalogs().getDataTypeFactory());
                write(() -> catalog.alterTable(path, definition, sql.ifTableExists()));
            }
        }
    }

    private void renameTable(SqlAlterTableRename sql, ObjectPath path, HiveCatalog catalog) throws Exception {
        ObjectPath target = objectPath(sql.fullNewTableName(), path.getDatabaseName());
        if (!path.getDatabaseName().equals(target.getDatabaseName())) {
            throw new UnsupportedOperationException("Renaming a table across databases is not supported");
        }
        write(() -> catalog.renameTable(path, target.getObjectName(), sql.ifTableExists()));
    }

    private void addPartitions(SqlAddPartitions sql, ObjectPath path, HiveCatalog catalog) throws Exception {
        List<CatalogPartitionSpec> specs = new ArrayList<>();
        List<CatalogPartition> definitions = new ArrayList<>();
        for (int i = 0; i < sql.getPartSpecs().size(); i++) {
            specs.add(new CatalogPartitionSpec(sql.getPartitionKVs(i)));
            definitions.add(new CatalogPartitionImpl(FlinkTableDdl.properties(sql.getPartProps().get(i)), null));
        }
        write(() -> {
            for (int i = 0; i < specs.size(); i++) {
                catalog.createPartition(path, specs.get(i), definitions.get(i), sql.ifPartitionNotExists());
            }
        });
    }

    private void dropPartitions(SqlDropPartitions sql, ObjectPath path, HiveCatalog catalog) throws Exception {
        List<CatalogPartitionSpec> specs = new ArrayList<>();
        for (int i = 0; i < sql.getPartSpecs().size(); i++) specs.add(new CatalogPartitionSpec(sql.getPartitionKVs(i)));
        write(() -> {
            for (CatalogPartitionSpec spec : specs) catalog.dropPartition(path, spec, sql.ifExists());
        });
    }

    private void alterPartition(SqlAlterTableOptions sql, ObjectPath path, HiveCatalog catalog) throws Exception {
        var spec = new CatalogPartitionSpec(sql.getPartitionKVs());
        CatalogPartition previous = catalog.getPartition(path, spec);
        Map<String, String> properties = new HashMap<>(previous.getProperties());
        properties.putAll(FlinkTableDdl.properties(sql.getPropertyList()));
        var definition = new CatalogPartitionImpl(properties, previous.getComment());
        write(() -> catalog.alterPartition(path, spec, definition, false));
    }

    private void createFunction(SqlCreateFunction sql, String database) throws Exception {
        String[] names = sql.getFunctionIdentifier();
        ObjectPath path = objectPath(names, database);
        List<ResourceUri> resources = sql.getResourceInfos().stream().map(SqlResource.class::cast)
                .map(resource -> new ResourceUri(ResourceType.valueOf(
                        resource.getResourceType().getValueAs(SqlResourceType.class).name()),
                        literal(resource.getResourcePath()))).toList();
        var definition = new CatalogFunctionImpl(literal(sql.getFunctionClassName()), language(sql.getFunctionLanguage()), resources);
        HiveCatalog catalog = catalogForDdl(database, names.length == 1);
        write(() -> catalog.createFunction(path, definition, sql.isIfNotExists()));
    }

    private void alterFunction(SqlAlterFunction sql, String database) throws Exception {
        String[] names = sql.getFunctionIdentifier();
        ObjectPath path = objectPath(names, database);
        var definition = new CatalogFunctionImpl(literal(sql.getFunctionClassName()), language(sql.getFunctionLanguage()));
        HiveCatalog catalog = catalogForDdl(database, names.length == 1);
        write(() -> catalog.alterFunction(path, definition, sql.isIfExists()));
    }

    private void dropFunction(SqlDropFunction sql, String database) throws Exception {
        String[] names = sql.getFunctionIdentifier();
        ObjectPath path = objectPath(names, database);
        HiveCatalog catalog = catalogForDdl(database, names.length == 1);
        write(() -> catalog.dropFunction(path, sql.getIfExists()));
    }

    private HiveCatalog catalogForDdl(String database, boolean requiresCurrentDatabase) {
        HiveCatalog catalog = hiveCatalog(catalogs());
        if (requiresCurrentDatabase && !catalog.databaseExists(database)) {
            throw new IllegalArgumentException("Database does not exist: " + database);
        }
        return catalog;
    }

    /** Only Catalog write calls belong here; SQL conversion and preparatory reads happen first. */
    private static void write(CatalogWrite write) throws Exception {
        try {
            write.execute();
        } catch (Exception e) {
            // Catalog writes can include internal reads. Be conservative once a write call has begun.
            if (hasTransportFailure(e)) {
                throw new IllegalStateException("METADATA_WRITE_OUTCOME_UNKNOWN: connection lost while writing HMS; "
                        + "read back the object before retrying this DDL", e);
            }
            throw e;
        }
    }

    /** Convenience entry point for callers without an already parsed statement. */
    public SqlProcessResult executeQuery(String sql, String database) throws Exception {
        return executeQuery(parseSql(sql), database);
    }

    public synchronized SqlProcessResult executeQuery(SqlNode node, String database) throws Exception {
        try {
            return switch (node) {
                case SqlShowCreateTable show -> showCreateTable(show, database);
                case SqlRichDescribeTable describe -> describeTable(describe, database);
                case SqlShowFunctions show -> showFunctions(show, database);
                default -> throw unsupported(node);
            };
        } catch (Exception e) {
            throw executionFailure(e);
        }
    }

    private SqlProcessResult showCreateTable(SqlShowCreateTable show, String database) throws Exception {
        ObjectPath path = objectPath(show.getTableName().names.toArray(String[]::new), database);
        CatalogManager manager = catalogs();
        var definition = FlinkTableDdl.resolve(hiveCatalog(manager).getTable(path), manager.getDataTypeFactory());
        String ddl = ShowCreateUtil.buildShowCreateTableRow(definition,
                ObjectIdentifier.of(Consts.HIVE_CATALOG_NAME, path.getDatabaseName(), path.getObjectName()), false);
        return SqlProcessResult.msg(ddl, "result");
    }

    private SqlProcessResult describeTable(SqlRichDescribeTable describe, String database) throws Exception {
        ObjectPath path = objectPath(describe.fullTableName(), database);
        CatalogManager manager = catalogs();
        var schema = FlinkTableDdl.resolve(hiveCatalog(manager).getTable(path), manager.getDataTypeFactory()).getResolvedSchema();
        return MetadataQueryUtils.describeTable(schema);
    }

    private SqlProcessResult showFunctions(SqlShowFunctions show, String database) throws Exception {
        String name = databaseName(show.fullDatabaseName(), database);
        return MetadataQueryUtils.showFunctions(show, hiveCatalog(catalogs()).listFunctions(name));
    }

    private CatalogManager catalogs() {
        if (catalogs == null) catalogs = Objects.requireNonNull(catalogFactory.get(), "Catalog factory returned null");
        return catalogs;
    }

    private static CatalogManager createCatalogs() {
        HiveConf conf = new HiveConf();
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, SqlRecConfigs.HIVE_METASTORE_URI.getValue());
        conf.set("hive.metastore.execute.setugi", SqlRecConfigs.EXECUTE_SET_UGI.getValue());
        return openCatalog(new HiveCatalog(Consts.HIVE_CATALOG_NAME, Consts.DEFAULT_SCHEMA_NAME, conf, Consts.HIVE_CLIENT_VERSION));
    }

    /** Open and take ownership of the catalog, cleaning it up if initialization fails. */
    static CatalogManager openCatalog(HiveCatalog catalog) {
        try {
            catalog.open();
            Configuration config = new Configuration();
            ClassLoader loader = FlinkHiveDdlAdapter.class.getClassLoader();
            // CatalogManager supplies Flink's type factory without initializing any planner.
            return CatalogManager.newBuilder().classLoader(loader).config(config)
                    .defaultCatalog(Consts.HIVE_CATALOG_NAME, catalog)
                    .catalogStoreHolder(CatalogStoreHolder.newBuilder().catalogStore(new GenericInMemoryCatalogStore())
                            .config(config).classloader(loader).build()).build();
        } catch (RuntimeException e) {
            try { catalog.close(); } catch (RuntimeException closeFailure) { e.addSuppressed(closeFailure); }
            throw e;
        }
    }

    private static HiveCatalog hiveCatalog(CatalogManager manager) {
        return (HiveCatalog) manager.getCatalog(Consts.HIVE_CATALOG_NAME).orElseThrow();
    }

    private static void requirePersistentDdl(SqlNode node) {
        if (node instanceof SqlCreateTable create) {
            if (create.isTemporary() || (create.getClass() != SqlCreateTable.class && !(create instanceof SqlCreateTableLike))) {
                throw unsupported(node);
            }
            FlinkTableDdl.requireSimpleSchema(create.getColumnList(), create.getWatermark().isPresent());
        } else if (node instanceof SqlAlterTable alter) {
            requireSupportedAlter(alter);
            if (alter instanceof SqlAlterTableSchema schema) {
                var columns = schema.getColumnPositions().getList().stream()
                        .map(position -> ((SqlTableColumnPosition) position).getColumn()).toList();
                FlinkTableDdl.requireSimpleSchema(columns, schema.getWatermark().isPresent());
            }
        } else if (node instanceof SqlDropTable drop) {
            if (drop.isTemporary()) throw unsupported(node);
        } else if (node instanceof SqlCreateFunction create) {
            if (create.isTemporary() || create.isSystemFunction()) throw unsupported(node);
        } else if (node instanceof SqlAlterFunction alter) {
            if (alter.isTemporary() || alter.isSystemFunction()) throw unsupported(node);
        } else if (node instanceof SqlDropFunction drop) {
            if (drop.isTemporary() || drop.isSystemFunction()) throw unsupported(node);
        } else if (!(node instanceof SqlCreateDatabase) && !(node instanceof SqlAlterDatabase)
                && !(node instanceof SqlDropDatabase)) {
            throw unsupported(node);
        }
    }

    private static void requireSupportedAlter(SqlAlterTable sql) {
        if (!(sql instanceof SqlAlterTableRename) && !(sql instanceof SqlAddPartitions)
                && !(sql instanceof SqlDropPartitions) && !(sql instanceof SqlAlterTableOptions)
                && !(sql instanceof SqlAlterTableReset) && !(sql instanceof SqlAlterTableAdd)
                && !(sql instanceof SqlAlterTableModify) && !(sql instanceof SqlAlterTableDropColumn)
                && !(sql instanceof SqlAlterTableRenameColumn) && !(sql instanceof SqlAlterTableDropPrimaryKey)
                && !(sql instanceof SqlAlterTableDropConstraint)) {
            throw unsupported(sql);
        }
    }

    private static String literal(SqlNode node) { return ((SqlLiteral) node).getValueAs(String.class); }

    private static FunctionLanguage language(String value) {
        return value == null ? FunctionLanguage.JAVA : FunctionLanguage.valueOf(value.toUpperCase(Locale.ROOT));
    }

    private static SqlNode parseSql(String sql) {
        try {
            return CompileManager.parseSql(sql);
        } catch (Exception e) {
            throw asRuntimeException(e);
        }
    }

    private RuntimeException executionFailure(Exception error) {
        if (hasTransportFailure(error)) {
            try { close(); } catch (RuntimeException closeFailure) { error.addSuppressed(closeFailure); }
        }
        return asRuntimeException(error);
    }

    private static RuntimeException asRuntimeException(Exception error) {
        return error instanceof RuntimeException runtime ? runtime : new TableException(error.getMessage(), error);
    }

    private static UnsupportedOperationException unsupported(SqlNode node) {
        return new UnsupportedOperationException("UNSUPPORTED_METADATA_DDL: " + node.getClass().getSimpleName()
                + "; only persistent table, database and UDF metadata operations are supported");
    }

    private static boolean hasTransportFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof TTransportException) return true;
        }
        return false;
    }

    /** Release the current connection. A later request opens a fresh one lazily. */
    @Override
    public synchronized void close() {
        CatalogManager previous = catalogs;
        catalogs = null;
        if (previous != null) previous.close();
    }

    @FunctionalInterface
    private interface CatalogWrite { void execute() throws Exception; }
}
