package com.sqlrec.db.remote;

import com.sqlrec.common.config.Consts;
import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.common.utils.HiveTableUtils;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Opt-in: requires an isolated HMS via HIVE_METASTORE_URI, but never a Gateway endpoint. */
@Tag("integration")
class FlinkHiveDdlIntegrationTest {
    @Test
    void createsAltersRenamesAndDropsTablesThroughRealHms() throws Exception {
        withCatalog((adapter, reader, database) -> {
            adapter.executeDdl("CREATE DATABASE " + database, "default");
            FlinkHiveDdlContract.tableLifecycle(adapter, reader, database);
        });
    }

    @Test
    void createsAltersAndDropsDatabasesAndFunctionsThroughRealHms(@TempDir Path directory) throws Exception {
        withCatalog((adapter, reader, database) ->
                FlinkHiveDdlContract.databaseAndFunctionLifecycle(adapter, reader, database, directory));
    }

    @Test
    void roundTripsThroughAnIndependentFlinkHiveClient() throws Exception {
        String database = "sqlrec_review_" + UUID.randomUUID().toString().replace("-", "");
        HiveConf conf = new HiveConf();
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, SqlRecConfigs.HIVE_METASTORE_URI.getValue());
        HiveCatalog independent = new HiveCatalog(Consts.HIVE_CATALOG_NAME, "default", conf,
                Consts.HIVE_CLIENT_VERSION);
        TableEnvironmentInternal independentEnvironment = (TableEnvironmentInternal) TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());
        try (FlinkHiveDdlAdapter adapter = new FlinkHiveDdlAdapter()) {
            independentEnvironment.registerCatalog(Consts.HIVE_CATALOG_NAME, independent);
            independentEnvironment.useCatalog(Consts.HIVE_CATALOG_NAME);
            try {
                adapter.executeDdl("CREATE DATABASE " + database, "default");
                String table = database + ".items";
                adapter.executeDdl("CREATE TABLE " + table + " (id BIGINT, category STRING, "
                        + "ts TIMESTAMP(3), "
                        + "PRIMARY KEY (id) NOT ENFORCED) "
                        + "COMMENT 'review' PARTITIONED BY (category) "
                        + "WITH ('connector'='filesystem', 'path'='file:///tmp/sqlrec-review', 'format'='json')", "default");
                ObjectPath path = new ObjectPath(database, "items");
                var definition = independent.getTable(path);
                assertEquals(3, definition.getUnresolvedSchema().getColumns().size());
                assertEquals("filesystem", definition.getOptions().get("connector"));
                assertFalse(definition.getOptions().keySet().stream().anyMatch(key -> key.startsWith("schema.")));
                assertEquals(3, HiveTableUtils.getMetadataFields(HmsClient.getTableObj(database, "items")).size());
                adapter.executeDdl("ALTER TABLE " + table + " SET ('path'='file:///tmp/sqlrec-review-v2')", "default");
                assertEquals("file:///tmp/sqlrec-review-v2", independent.getTable(path).getOptions().get("path"));
                assertEquals(3, independent.getTable(path).getUnresolvedSchema().getColumns().size());
                // Reverse direction: an independent Flink client persists a copy, SQLRec reads and alters it.
                ObjectPath copy = new ObjectPath(database, "items_copy");
                independent.createTable(copy, independentEnvironment.getCatalogManager()
                        .resolveCatalogBaseTable(independent.getTable(path)), false);
                assertEquals(3, HiveTableUtils.getMetadataFields(HmsClient.getTableObj(database, "items_copy")).size());
                adapter.executeDdl("ALTER TABLE " + database + ".items_copy RENAME TO items_renamed", "default");
                assertTrue(independent.tableExists(new ObjectPath(database, "items_renamed")));
                assertFalse(independent.tableExists(copy));

                adapter.executeDdl("CREATE FUNCTION " + database
                        + ".review_python AS 'review_python' LANGUAGE PYTHON", "default");
                var function = independent.getFunction(new ObjectPath(database, "review_python"));
                assertEquals(org.apache.flink.table.catalog.FunctionLanguage.PYTHON, function.getFunctionLanguage());
                assertTrue(function.getFunctionResources().isEmpty());
                adapter.executeDdl("DROP FUNCTION " + database + ".review_python", "default");
                assertFalse(independent.functionExists(new ObjectPath(database, "review_python")));
                adapter.executeDdl("DROP TABLE " + table, "default");
                assertFalse(independent.tableExists(path));
                adapter.executeDdl("DROP TABLE " + database + ".items_renamed", "default");
                assertFalse(independent.tableExists(new ObjectPath(database, "items_renamed")));
            } finally {
                adapter.executeDdl("DROP DATABASE IF EXISTS " + database + " CASCADE", "default");
            }
        } finally {
            try {
                independentEnvironment.getCatalogManager().close();
            } finally {
                independent.close();
            }
        }
    }

    @FunctionalInterface
    private interface Scenario {
        void run(FlinkHiveDdlAdapter adapter, HiveCatalog reader, String database) throws Exception;
    }

    private static void withCatalog(Scenario scenario) throws Exception {
        String database = "sqlrec_review_" + UUID.randomUUID().toString().replace("-", "");
        HiveConf conf = new HiveConf();
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, SqlRecConfigs.HIVE_METASTORE_URI.getValue());
        HiveCatalog reader = new HiveCatalog(Consts.HIVE_CATALOG_NAME, "default", conf, Consts.HIVE_CLIENT_VERSION);
        try (FlinkHiveDdlAdapter adapter = new FlinkHiveDdlAdapter()) {
            reader.open();
            try {
                scenario.run(adapter, reader, database);
            } finally {
                adapter.executeDdl("DROP DATABASE IF EXISTS " + database + " CASCADE", "default");
            }
        } finally {
            reader.close();
        }
    }
}
