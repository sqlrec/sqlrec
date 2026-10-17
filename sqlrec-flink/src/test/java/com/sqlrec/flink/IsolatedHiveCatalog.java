package com.sqlrec.flink;

import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

/** Embedded test catalog; never inherits a remote metastore or warehouse. */
final class IsolatedHiveCatalog implements AutoCloseable {
    private final HiveCatalog catalog;
    private final String databaseUrl = "jdbc:derby:memory:udf_test_"
            + UUID.randomUUID().toString().replace("-", "");
    private final String warehouse;
    private boolean opened;

    IsolatedHiveCatalog(HiveConf conf, Path warehouseDir) {
        warehouse = warehouseDir.toAbsolutePath().toUri().toString();
        conf.set("hive.metastore.uris", "");
        conf.set("metastore.thrift.uris", "");
        conf.set("javax.jdo.option.ConnectionURL", databaseUrl + ";create=true");
        conf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        conf.set("javax.jdo.option.ConnectionUserName", "APP");
        conf.set("javax.jdo.option.ConnectionPassword", "");
        conf.set("hive.metastore.ds.connection.url.hook", "");
        conf.set("metastore.ds.connection.url.hook", "");
        conf.set("hive.metastore.warehouse.dir", warehouse);
        conf.set("metastore.warehouse.dir", warehouse);
        conf.set("hive.metastore.warehouse.external.dir", warehouse);
        conf.set("metastore.warehouse.external.dir", warehouse);
        conf.set("fs.defaultFS", "file:///");
        conf.set("datanucleus.connectionPoolingType", "None");
        conf.setBoolean("hive.metastore.schema.verification", false);
        conf.setBoolean("metastore.schema.verification", false);
        conf.setBoolean("datanucleus.schema.autoCreateAll", true);
        conf.setBoolean("hive.metastore.execute.setugi", false);
        conf.setBoolean("metastore.execute.setugi", false);
        catalog = new HiveCatalog("hive", "default", conf, "3.1.3", true);
    }

    HiveCatalog open() {
        verifyIsolation();
        catalog.open();
        opened = true;
        return catalog;
    }

    HiveConf getHiveConf() {
        return catalog.getHiveConf();
    }

    void verifyIsolation() {
        HiveConf conf = getHiveConf();
        if (!"".equals(conf.get("hive.metastore.uris"))
                || !"".equals(MetastoreConf.getVar(conf, MetastoreConf.ConfVars.THRIFT_URIS))
                || !(databaseUrl + ";create=true").equals(
                        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY))
                || !"org.apache.derby.jdbc.EmbeddedDriver".equals(
                        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER))
                || !warehouse.equals(conf.get("hive.metastore.warehouse.dir"))
                || !warehouse.equals(MetastoreConf.getVar(conf, MetastoreConf.ConfVars.WAREHOUSE))) {
            throw new IllegalStateException("UDF tests require an isolated embedded Derby metastore and local warehouse");
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            catalog.close();
        } finally {
            if (opened) {
                opened = false;
                // Derby reports successful removal of an in-memory database as SQLState 08006.
                try (Connection ignored = DriverManager.getConnection(databaseUrl + ";drop=true")) {
                    throw new SQLException("Derby did not report removal of the test database");
                } catch (SQLException e) {
                    if (!"08006".equals(e.getSQLState())) {
                        throw e;
                    }
                }
            }
        }
    }
}
