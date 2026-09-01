package com.sqlrec.schema;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.remote.HmsSchemaAccess;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("integration")
public class HmsSchemaUpdateTest {
    private static final String DATABASE = "default";

    private HiveMetaStoreClient hmsClient;
    private String tableName;

    @BeforeEach
    void setUp() throws Exception {
        Configuration hiveConf = new Configuration();
        hiveConf.set(
                HiveConf.ConfVars.METASTOREURIS.toString(),
                SqlRecConfigs.HIVE_METASTORE_URI.getValue()
        );
        hiveConf.set(
                MetastoreConf.ConfVars.EXECUTE_SET_UGI.toString(),
                SqlRecConfigs.EXECUTE_SET_UGI.getValue()
        );
        hmsClient = new HiveMetaStoreClient(hiveConf);
        tableName = "sqlrec_schema_update_" + UUID.randomUUID().toString().replace("-", "");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (hmsClient != null) {
            try {
                hmsClient.dropTable(DATABASE, tableName, false, true);
            } finally {
                hmsClient.close();
            }
        }
    }

    @Test
    void refreshesExistingTableWhenHmsModificationTimeChanges() throws Exception {
        org.apache.hadoop.hive.metastore.api.Table hmsTable = createHmsTable();
        hmsClient.createTable(hmsTable);

        MetadataAccess metadataAccess = new MetadataAccess(new HmsSchemaAccess(), null, null);
        HmsSchema schema = new HmsSchema(DATABASE, metadataAccess, 1L, false);
        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        Table first = schema.getTable(tableName);
        assertNotNull(first);
        assertEquals(List.of("id"), first.getRowType(typeFactory).getFieldNames());

        // A refresh without an HMS change must continue to reuse the cached Calcite table.
        Thread.sleep(5L);
        Table unchanged = schema.getTable(tableName);
        assertSame(first, unchanged);

        org.apache.hadoop.hive.metastore.api.Table altered = hmsClient.getTable(DATABASE, tableName);
        List<FieldSchema> columns = new ArrayList<>(altered.getSd().getCols());
        columns.add(new FieldSchema("name", "string", null));
        altered.getSd().setCols(columns);
        Map<String, String> parameters = new HashMap<>(altered.getParameters());
        long previousDdlTime = Long.parseLong(parameters.get("transient_lastDdlTime"));
        parameters.put("transient_lastDdlTime", Long.toString(previousDdlTime + 1));
        altered.setParameters(parameters);
        hmsClient.alter_table(DATABASE, tableName, altered);

        Thread.sleep(5L);
        Table refreshed = schema.getTable(tableName);
        assertNotNull(refreshed);
        assertNotSame(first, refreshed);
        assertEquals(List.of("id", "name"), refreshed.getRowType(typeFactory).getFieldNames());
    }

    private org.apache.hadoop.hive.metastore.api.Table createHmsTable() {
        long nowSeconds = System.currentTimeMillis() / 1000;

        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(tableName);
        serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        serdeInfo.setParameters(new HashMap<>());

        StorageDescriptor storageDescriptor = new StorageDescriptor();
        storageDescriptor.setCols(new ArrayList<>(List.of(
                new FieldSchema("id", "bigint", null)
        )));
        storageDescriptor.setLocation("file:///tmp/" + tableName);
        storageDescriptor.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
        storageDescriptor.setCompressed(false);
        storageDescriptor.setNumBuckets(-1);
        storageDescriptor.setSerdeInfo(serdeInfo);
        storageDescriptor.setBucketCols(new ArrayList<>());
        storageDescriptor.setSortCols(new ArrayList<>());
        storageDescriptor.setParameters(new HashMap<>());

        Map<String, String> parameters = new HashMap<>();
        parameters.put("EXTERNAL", "TRUE");
        parameters.put("flink.connector", "filesystem");
        parameters.put("flink.format", "csv");
        parameters.put("flink.schema.primary-key.columns", "id");
        parameters.put("transient_lastDdlTime", Long.toString(nowSeconds));

        org.apache.hadoop.hive.metastore.api.Table table =
                new org.apache.hadoop.hive.metastore.api.Table();
        table.setTableName(tableName);
        table.setDbName(DATABASE);
        table.setOwner("sqlrec-integration-test");
        table.setCreateTime((int) nowSeconds);
        table.setLastAccessTime(0);
        table.setRetention(0);
        table.setSd(storageDescriptor);
        table.setPartitionKeys(new ArrayList<>());
        table.setParameters(parameters);
        table.setViewOriginalText(null);
        table.setViewExpandedText(null);
        table.setTableType("EXTERNAL_TABLE");
        return table;
    }
}
