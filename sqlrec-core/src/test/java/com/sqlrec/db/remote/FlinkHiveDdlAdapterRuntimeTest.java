package com.sqlrec.db.remote;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Runs the metadata adapter and HiveCatalog with HMS I/O simulated; an independent Flink planner is the compatibility oracle. */
class FlinkHiveDdlAdapterRuntimeTest {
    @Test
    void persistsAndReadsFlinkTableDefinitionsWithoutGateway() throws Exception {
        try (var factory = mockStatic(HiveMetastoreClientFactory.class);
             Metastore metastore = new Metastore(factory)) {
            metastore.adapter.executeDdl("CREATE DATABASE review", "default");
            FlinkHiveDdlContract.tableLifecycle(metastore.adapter, metastore.reader, "review");
            metastore.adapter.executeDdl("DROP DATABASE review", "default");
            assertFalse(metastore.reader.databaseExists("review"));
        }
    }

    @Test
    void qualifiedDdlDoesNotDependOnTheSessionDatabase() throws Exception {
        try (var factory = mockStatic(HiveMetastoreClientFactory.class);
             Metastore metastore = new Metastore(factory)) {
            String sessionDatabase = "missing_session";
            metastore.adapter.executeDdl("CREATE DATABASE review", sessionDatabase);
            metastore.adapter.executeDdl("CREATE TABLE hive.review.items (id BIGINT) "
                    + "WITH ('connector'='filesystem', 'path'='file:///tmp/review', 'format'='json')", sessionDatabase);
            metastore.adapter.executeDdl("ALTER TABLE review.items ADD note STRING", sessionDatabase);
            metastore.adapter.executeDdl("CREATE TABLE review.copy LIKE hive.review.items", sessionDatabase);
            metastore.adapter.executeDdl("ALTER TABLE review.copy RENAME TO renamed", sessionDatabase);
            assertTrue(metastore.tables.containsKey("review.renamed"));
            metastore.adapter.executeDdl("DROP TABLE review.renamed", sessionDatabase);
            metastore.adapter.executeDdl("CREATE FUNCTION review.udf AS 'example.Udf'", sessionDatabase);
            metastore.adapter.executeDdl("ALTER FUNCTION review.udf AS 'example.UpdatedUdf'", sessionDatabase);
            metastore.adapter.executeDdl("DROP FUNCTION review.udf", sessionDatabase);
            verify(metastore.client, never()).getDatabase(sessionDatabase);

            for (String sql : new String[]{"CREATE TABLE local_table (id INT)",
                    "CREATE TABLE review.copy LIKE items", "ALTER TABLE items ADD note STRING"}) {
                assertThrows(IllegalArgumentException.class,
                        () -> metastore.adapter.executeDdl(sql, sessionDatabase), sql);
            }
            assertFalse(metastore.tables.containsKey("review.copy"));
            assertFalse(metastore.tables.containsKey("missing_session.local_table"));
        }
    }

    @Test
    void persistsAltersAndDropsDatabasesAndFunctions(@TempDir Path directory) throws Exception {
        try (var factory = mockStatic(HiveMetastoreClientFactory.class);
             Metastore metastore = new Metastore(factory)) {
            FlinkHiveDdlContract.databaseAndFunctionLifecycle(metastore.adapter, metastore.reader, "review", directory);
        }
    }

    @Test
    void producesTheSameTableMetadataAsAnIndependentFlinkPlanner() throws Exception {
        try (var factory = mockStatic(HiveMetastoreClientFactory.class);
             Metastore metastore = new Metastore(factory)) {
            metastore.adapter.executeDdl("CREATE DATABASE review", "default");
            var official = (org.apache.flink.table.api.internal.TableEnvironmentInternal)
                    org.apache.flink.table.api.TableEnvironment.create(
                            org.apache.flink.table.api.EnvironmentSettings.inStreamingMode());
            official.registerCatalog("hive", metastore.reader);
            official.useCatalog("hive");
            official.useDatabase("review");
            try {
                String[] statements = {
                        "CREATE TABLE %s (id BIGINT, category STRING, amount DECIMAL(10,2), "
                                + "nested ROW<`value` INT, labels ARRAY<STRING>>, "
                                + "attrs MAP<STRING, BIGINT>, ts TIMESTAMP_LTZ(3), flag BOOLEAN, "
                                + "tiny TINYINT, small SMALLINT, number INT, f FLOAT, d DOUBLE, "
                                + "date_value DATE, clock_value TIME, timestamp_value TIMESTAMP, binary_value BYTES, "
                                + "text_value CHAR(10), bytes_value VARBINARY(16), "
                                + "source STRING METADATA FROM 'origin' VIRTUAL, PRIMARY KEY (id) NOT ENFORCED) "
                                + "COMMENT 'review' PARTITIONED BY (category) "
                                + "WITH ('connector'='filesystem', 'path'='file:///tmp/review', 'format'='json')",
                        "ALTER TABLE %s ADD note VARCHAR(20) COMMENT 'note' FIRST",
                        "ALTER TABLE %s MODIFY note VARCHAR(100) COMMENT 'updated' AFTER amount",
                        "ALTER TABLE %s RENAME note TO memo",
                        "ALTER TABLE %s SET ('path'='file:///tmp/updated', 'review'='value')",
                        "ALTER TABLE %s RESET ('review')",
                        "ALTER TABLE %s DROP memo",
                        "ALTER TABLE %s DROP PRIMARY KEY",
                        "ALTER TABLE %s ADD CONSTRAINT review_pk PRIMARY KEY (id) NOT ENFORCED",
                        "ALTER TABLE %s MODIFY CONSTRAINT updated_pk PRIMARY KEY (id) NOT ENFORCED",
                        "ALTER TABLE %s RENAME id TO identity_id",
                        "ALTER TABLE %s ADD implicit_meta STRING METADATA",
                        "ALTER TABLE %s RENAME implicit_meta TO implicit_meta_renamed"
                };
                for (String statement : statements) {
                    metastore.adapter.executeDdl(statement.formatted("review.candidate"), "review");
                    official.executeSql(statement.formatted("review.official"));
                    assertEquals(metastore.tables.get("review.official").getParameters(),
                            metastore.tables.get("review.candidate").getParameters(), statement);
                    assertEquals(metastore.tables.get("review.official").getSd(),
                            metastore.tables.get("review.candidate").getSd(), statement);
                }
                metastore.adapter.executeDdl("CREATE TABLE review.candidate_copy LIKE review.candidate", "review");
                official.executeSql("CREATE TABLE review.official_copy LIKE review.official");
                assertEquals(metastore.tables.get("review.official_copy").getParameters(),
                        metastore.tables.get("review.candidate_copy").getParameters());
                String[] copies = {
                        "CREATE TABLE %s WITH ('connector'='filesystem', 'path'='file:///tmp/excluded', 'format'='json') "
                                + "LIKE %s (EXCLUDING ALL)",
                        "CREATE TABLE %s LIKE %s (EXCLUDING ALL INCLUDING OPTIONS INCLUDING METADATA INCLUDING CONSTRAINTS INCLUDING PARTITIONS)",
                        "CREATE TABLE %s WITH ('path'='file:///tmp/copy') LIKE %s (OVERWRITING OPTIONS)",
                        "CREATE TABLE %s (source STRING METADATA FROM 'changed' VIRTUAL) LIKE %s (OVERWRITING METADATA)",
                        "CREATE TABLE %s PARTITIONED BY (identity_id) LIKE %s (EXCLUDING PARTITIONS)",
                        "CREATE TABLE %s WITH ('copy-option'='value') LIKE %s (INCLUDING OPTIONS)"
                };
                for (int i = 0; i < copies.length; i++) {
                    String candidate = "review.candidate_copy_" + i;
                    String reference = "review.official_copy_" + i;
                    metastore.adapter.executeDdl(copies[i].formatted(candidate, "review.candidate"), "review");
                    official.executeSql(copies[i].formatted(reference, "review.official"));
                    assertEquals(metastore.tables.get(reference).getParameters(), metastore.tables.get(candidate).getParameters(), copies[i]);
                    assertEquals(metastore.tables.get(reference).getSd(), metastore.tables.get(candidate).getSd(), copies[i]);
                }
                for (String definition : new String[]{
                        "(id INT, extra AS id + 1)",
                        "(ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)"}) {
                    official.executeSql("CREATE TABLE review.unsupported " + definition
                            + " WITH ('connector'='filesystem', 'path'='file:///tmp/review', 'format'='json')");
                    Table before = metastore.tables.get("review.unsupported").deepCopy();
                    assertThrows(UnsupportedOperationException.class, () -> metastore.adapter.executeDdl(
                            "ALTER TABLE review.unsupported SET ('path'='file:///tmp/changed')", "review"));
                    assertThrows(UnsupportedOperationException.class, () -> metastore.adapter.executeQuery(
                            "SHOW CREATE TABLE review.unsupported", "review"));
                    assertEquals(before, metastore.tables.get("review.unsupported"));
                    official.executeSql("DROP TABLE review.unsupported");
                }
                // A definition created by the independent official client can be read and altered locally.
                metastore.adapter.executeDdl("ALTER TABLE review.official SET ('path'='file:///tmp/local-change')", "review");
                assertEquals("file:///tmp/local-change", metastore.reader.getTable(
                        new org.apache.flink.table.catalog.ObjectPath("review", "official")).getOptions().get("path"));
            } finally {
                official.getCatalogManager().close();
            }
        }
    }

    @Test
    void rejectsUnsupportedAndInvalidChangesWithoutModifyingHms() throws Exception {
        try (var factory = mockStatic(HiveMetastoreClientFactory.class);
             Metastore metastore = new Metastore(factory)) {
            metastore.adapter.executeDdl("CREATE DATABASE review", "default");
            metastore.adapter.executeDdl("CREATE TABLE review.items (id BIGINT, category STRING, "
                    + "PRIMARY KEY (id) NOT ENFORCED) PARTITIONED BY (category) "
                    + "WITH ('connector'='filesystem', 'path'='file:///tmp/review', 'format'='json')", "review");
            Table before = metastore.tables.get("review.items").deepCopy();
            for (String sql : new String[]{"ALTER TABLE review.items ADD extra AS id + 1",
                    "ALTER TABLE review.items ADD WATERMARK FOR id AS id",
                    "ALTER TABLE review.items DROP id", "ALTER TABLE review.items DROP category",
                    "ALTER TABLE review.items ADD category STRING", "ALTER TABLE review.items RESET ('connector')",
                    "ALTER TABLE review.items MODIFY missing STRING", "ALTER TABLE review.items RENAME id TO category"}) {
                assertThrows(RuntimeException.class, () -> metastore.adapter.executeDdl(sql, "review"), sql);
                assertEquals(before, metastore.tables.get("review.items"), sql);
            }
            verify(metastore.client, never()).alter_table(anyString(), anyString(), any(Table.class));
        }
    }

    @Test
    void showsFunctionsWithFlinkLikeFilteringAndPreservesFunctionLanguages() throws Exception {
        try (var factory = mockStatic(HiveMetastoreClientFactory.class);
             Metastore metastore = new Metastore(factory)) {
            metastore.adapter.executeDdl("CREATE DATABASE review", "default");
            metastore.adapter.executeDdl("CREATE FUNCTION review.python_udf AS 'module.udf' LANGUAGE PYTHON", "review");
            var function = metastore.reader.getFunction(new org.apache.flink.table.catalog.ObjectPath("review", "python_udf"));
            assertEquals(org.apache.flink.table.catalog.FunctionLanguage.PYTHON, function.getFunctionLanguage());
            assertEquals("module.udf", function.getClassName());
            assertTrue(function.getFunctionResources().isEmpty());
            var rows = metastore.adapter.executeQuery("SHOW USER FUNCTIONS IN review ILIKE 'PYTHON%'", "default")
                    .getEnumerable().toList();
            assertEquals(1, rows.size());
            assertEquals("python_udf", rows.get(0)[0]);
            assertTrue(metastore.adapter.executeQuery("SHOW USER FUNCTIONS IN review NOT LIKE 'python%'", "default")
                    .getEnumerable().toList().isEmpty());
            assertTrue(metastore.adapter.executeQuery("SHOW FUNCTIONS IN review ILIKE 'abs'", "default")
                    .getEnumerable().toList().stream().anyMatch(row -> row[0].toString().equalsIgnoreCase("abs")));
        }
    }

    private static final class Metastore implements AutoCloseable {
        private final HiveMetastoreClientWrapper client = mock(HiveMetastoreClientWrapper.class);
        private final Map<String, Database> databases = new HashMap<>();
        private final Map<String, Table> tables = new HashMap<>();
        private final Map<String, Function> functions = new HashMap<>();
        private final HiveCatalog reader;
        private final FlinkHiveDdlAdapter adapter = new FlinkHiveDdlAdapter();

        private Metastore(MockedStatic<HiveMetastoreClientFactory> factory) throws Exception {
            databases.put("default", new Database("default", "", "file:///tmp", Map.of()));
            when(client.getDatabase(anyString())).thenAnswer(call -> {
                Database database = databases.get(call.<String>getArgument(0));
                if (database == null) throw new NoSuchObjectException();
                return database.deepCopy();
            });
            when(client.getAllDatabases()).thenAnswer(call -> databases.keySet().stream().sorted().toList());
            doAnswer(call -> {
                Database database = call.getArgument(0);
                if (databases.containsKey(database.getName())) throw new AlreadyExistsException();
                databases.put(database.getName(), database.deepCopy());
                return null;
            }).when(client).createDatabase(any(Database.class));
            doAnswer(call -> {
                String name = call.getArgument(0);
                if (!databases.containsKey(name)) throw new NoSuchObjectException();
                databases.put(name, call.<Database>getArgument(1).deepCopy());
                return null;
            }).when(client).alterDatabase(anyString(), any(Database.class));
            doAnswer(call -> {
                String name = call.getArgument(0);
                if (!databases.containsKey(name) && !call.<Boolean>getArgument(2)) throw new NoSuchObjectException();
                boolean nonempty = tables.keySet().stream().anyMatch(key -> key.startsWith(name + "."));
                if (nonempty && !call.<Boolean>getArgument(3)) throw new InvalidOperationException("database is not empty");
                databases.remove(name);
                tables.keySet().removeIf(key -> key.startsWith(name + "."));
                functions.keySet().removeIf(key -> key.startsWith(name + "."));
                return null;
            }).when(client).dropDatabase(anyString(), anyBoolean(), anyBoolean(), anyBoolean());
            when(client.getTable(anyString(), anyString())).thenAnswer(call -> {
                Table table = tables.get(call.getArgument(0) + "." + call.getArgument(1));
                if (table == null) throw new NoSuchObjectException();
                return table.deepCopy();
            });
            when(client.tableExists(anyString(), anyString())).thenAnswer(call ->
                    tables.containsKey(call.getArgument(0) + "." + call.getArgument(1)));
            doAnswer(call -> {
                Table table = call.getArgument(0);
                String key = table.getDbName() + "." + table.getTableName();
                if (tables.containsKey(key)) throw new AlreadyExistsException();
                tables.put(key, table.deepCopy());
                return null;
            }).when(client).createTable(any(Table.class));
            doAnswer(call -> {
                String key = call.getArgument(0) + "." + call.getArgument(1);
                if (!tables.containsKey(key)) throw new NoSuchObjectException();
                Table table = call.getArgument(2);
                tables.remove(key);
                tables.put(table.getDbName() + "." + table.getTableName(), table.deepCopy());
                return null;
            }).when(client).alter_table(anyString(), anyString(), any(Table.class));
            doAnswer(call -> {
                Table removed = tables.remove(call.getArgument(0) + "." + call.getArgument(1));
                if (removed == null && !call.<Boolean>getArgument(3)) throw new NoSuchObjectException();
                return null;
            }).when(client).dropTable(anyString(), anyString(), anyBoolean(), anyBoolean());
            when(client.getFunctions(anyString(), anyString())).thenAnswer(call -> functions.values().stream()
                    .filter(function -> function.getDbName().equals(call.getArgument(0)))
                    .map(Function::getFunctionName).sorted().toList());
            when(client.getFunction(anyString(), anyString())).thenAnswer(call -> {
                Function function = functions.get(call.getArgument(0) + "." + call.getArgument(1));
                if (function == null) throw new NoSuchObjectException();
                return function.deepCopy();
            });
            doAnswer(call -> {
                Function function = call.getArgument(0);
                String key = function.getDbName() + "." + function.getFunctionName();
                if (functions.containsKey(key)) throw new AlreadyExistsException();
                functions.put(key, function.deepCopy());
                return null;
            }).when(client).createFunction(any(Function.class));
            doAnswer(call -> {
                String key = call.getArgument(0) + "." + call.getArgument(1);
                if (!functions.containsKey(key)) throw new NoSuchObjectException();
                functions.put(key, call.<Function>getArgument(2).deepCopy());
                return null;
            }).when(client).alterFunction(anyString(), anyString(), any(Function.class));
            doAnswer(call -> {
                if (functions.remove(call.getArgument(0) + "." + call.getArgument(1)) == null) {
                    throw new NoSuchObjectException();
                }
                return null;
            }).when(client).dropFunction(anyString(), anyString());

            factory.when(() -> HiveMetastoreClientFactory.create(any(HiveConf.class), eq(Consts.HIVE_CLIENT_VERSION)))
                    .thenReturn(client);
            HiveConf conf = new HiveConf();
            conf.set(HiveConf.ConfVars.METASTOREURIS.varname, SqlRecConfigs.HIVE_METASTORE_URI.getValue());
            reader = new HiveCatalog(Consts.HIVE_CATALOG_NAME, "default", conf, Consts.HIVE_CLIENT_VERSION);
            reader.open();
        }

        @Override
        public void close() throws Exception {
            try {
                adapter.close();
            } finally {
                reader.close();
            }
        }
    }
}
