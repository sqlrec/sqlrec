package com.sqlrec.schema;

import com.sqlrec.common.utils.SilenceLoggers;
import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.udf.UdfManager;
import com.sqlrec.utils.TableFactoryUtils;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class MetadataSchemaTest {
    @Test
    @SilenceLoggers(TableFactoryUtils.class)
    void unsupportedTableDoesNotPreventLoadingOtherTables() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var unsupported = table("unsupported");
        var supported = table("supported");
        when(metadata.getTables("db")).thenReturn(List.of(unsupported, supported));
        HmsTableFactory connector = mock(HmsTableFactory.class);
        Table loaded = mock(Table.class);
        when(connector.getTableFromHmsTable(unsupported)).thenThrow(new UnsupportedOperationException("unsupported table definition"));
        when(connector.getTableFromHmsTable(supported)).thenReturn(loaded);
        try (var factories = mockStatic(TableFactoryUtils.class)) {
            factories.when(() -> TableFactoryUtils.getTableFactory("review")).thenReturn(connector);
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(unsupported)).thenCallRealMethod();
            factories.when(() -> TableFactoryUtils.getTableFromHmsTable(supported)).thenCallRealMethod();
            HmsSchema schema = new HmsSchema("db", metadata);
            assertNull(schema.getTable("unsupported"));
            assertSame(loaded, schema.getTable("supported"));
        }
    }

    @Test
    @SilenceLoggers(HmsSchema.class)
    void missingUdfDependencyDoesNotPreventLoadingOtherFunctions() throws Exception {
        MetadataAccess metadata = mock(MetadataAccess.class);
        var broken = function("broken", "example.Broken");
        var healthy = function("healthy", "example.Healthy");
        when(metadata.getFunctions("db")).thenReturn(List.of(broken, healthy));
        ScalarFunction loaded = mock(ScalarFunction.class);
        try (var udf = mockStatic(UdfManager.class)) {
            udf.when(() -> UdfManager.createScalarFunction(anyString())).thenReturn(loaded);
            udf.when(() -> UdfManager.createScalarFunction("example.Broken"))
                    .thenThrow(new NoClassDefFoundError("missing dependency"));
            HmsSchema schema = new HmsSchema("db", metadata);
            assertTrue(schema.getFunctions("broken").isEmpty());
            assertTrue(schema.getFunctions("healthy").contains(loaded));
        }
    }

    private static org.apache.hadoop.hive.metastore.api.Table table(String name) {
        var table = new org.apache.hadoop.hive.metastore.api.Table();
        table.setTableName(name);
        table.setDbName("db");
        table.putToParameters("flink.connector", "review");
        return table;
    }

    private static org.apache.hadoop.hive.metastore.api.Function function(String name, String className) {
        var function = new org.apache.hadoop.hive.metastore.api.Function();
        function.setFunctionName(name);
        function.setClassName(className);
        return function;
    }
}
