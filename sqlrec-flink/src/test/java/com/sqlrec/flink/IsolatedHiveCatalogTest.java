package com.sqlrec.flink;

import com.sqlrec.udf.scalar.IpFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class IsolatedHiveCatalogTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void overridesBothRemoteUriAliasesDatabaseAndWarehouse() throws Exception {
        HiveConf inherited = pollutedConfiguration();
        try (IsolatedHiveCatalog isolated = new IsolatedHiveCatalog(inherited, temporaryDirectory)) {
            HiveConf conf = isolated.getHiveConf();
            assertEquals("", conf.get("hive.metastore.uris"));
            assertEquals("", conf.get("metastore.thrift.uris"));
            assertEquals("", MetastoreConf.getVar(conf, MetastoreConf.ConfVars.THRIFT_URIS));
            assertTrue(MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY)
                    .startsWith("jdbc:derby:memory:udf_test_"));
            assertEquals("org.apache.derby.jdbc.EmbeddedDriver",
                    MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER));
            String warehouse = temporaryDirectory.toAbsolutePath().toUri().toString();
            assertEquals(warehouse, conf.get("hive.metastore.warehouse.dir"));
            assertEquals(warehouse, MetastoreConf.getVar(conf, MetastoreConf.ConfVars.WAREHOUSE));
            assertDoesNotThrow(isolated::verifyIsolation);
        }
    }

    @Test
    void rejectsRemoteUriBeforeOpeningCatalog() throws Exception {
        try (IsolatedHiveCatalog isolated = new IsolatedHiveCatalog(new HiveConf(), temporaryDirectory)) {
            isolated.getHiveConf().set("metastore.thrift.uris", "thrift://127.0.0.1:1");
            assertThrows(IllegalStateException.class, isolated::open);
        }
    }

    @Test
    void rejectsChangedDatabaseAndWarehouseBeforeOpeningCatalog() throws Exception {
        try (IsolatedHiveCatalog isolated = new IsolatedHiveCatalog(new HiveConf(), temporaryDirectory)) {
            String database = isolated.getHiveConf().get("javax.jdo.option.ConnectionURL");
            isolated.getHiveConf().set("javax.jdo.option.ConnectionURL", "jdbc:postgresql://127.0.0.1:1/shared");
            assertThrows(IllegalStateException.class, isolated::open);
            isolated.getHiveConf().set("javax.jdo.option.ConnectionURL", database);
            isolated.getHiveConf().set("metastore.warehouse.dir", "hdfs://127.0.0.1:1/shared");
            assertThrows(IllegalStateException.class, isolated::open);
        }
    }

    @Test
    void functionDefinitionsDoNotLeakBetweenEmbeddedCatalogs() throws Exception {
        ObjectPath function = new ObjectPath("default", "isolated_ip");
        try (IsolatedHiveCatalog first = new IsolatedHiveCatalog(
                pollutedConfiguration(), temporaryDirectory.resolve("first"))) {
            var catalog = first.open();
            catalog.createFunction(function, new CatalogFunctionImpl(IpFunction.class.getName()), false);
            assertTrue(catalog.functionExists(function));
        }
        try (IsolatedHiveCatalog second = new IsolatedHiveCatalog(
                pollutedConfiguration(), temporaryDirectory.resolve("second"))) {
            assertFalse(second.open().functionExists(function));
        }
    }

    private static HiveConf pollutedConfiguration() {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", "thrift://127.0.0.1:1");
        conf.set("metastore.thrift.uris", "thrift://127.0.0.1:1");
        conf.set("javax.jdo.option.ConnectionURL", "jdbc:postgresql://127.0.0.1:1/shared");
        conf.set("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver");
        conf.set("hive.metastore.warehouse.dir", "hdfs://127.0.0.1:1/shared");
        conf.set("metastore.warehouse.dir", "hdfs://127.0.0.1:1/shared");
        return conf;
    }
}
