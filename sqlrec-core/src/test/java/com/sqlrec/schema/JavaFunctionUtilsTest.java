package com.sqlrec.schema;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.sqlrec.db.MetadataAccess;
import com.sqlrec.db.MetadataAccessFactory;
import com.sqlrec.db.SchemaAccess;
import com.sqlrec.db.local.InMemoryStoreAccess;
import com.sqlrec.db.local.LocalHdfsAccess;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class JavaFunctionUtilsTest {
    private MetadataAccess savedMetadataAccess;
    private final AtomicReference<Function> remoteFunction = new AtomicReference<>();

    @BeforeEach
    void setUp() throws Exception {
        Field instanceField = getMetadataAccessField();
        savedMetadataAccess = (MetadataAccess) instanceField.get(null);
        instanceField.set(null, new MetadataAccess(
                new MutableFunctionSchemaAccess(remoteFunction),
                new InMemoryStoreAccess(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
                new LocalHdfsAccess()
        ));

        JavaFunctionUtils.setSkipHmsQuery(false);
        JavaFunctionUtils.invalidateCache();
    }

    @AfterEach
    void tearDown() throws Exception {
        JavaFunctionUtils.invalidateCache();
        JavaFunctionUtils.setSkipHmsQuery(false);
        getMetadataAccessField().set(null, savedMetadataAccess);
    }

    @Test
    void refreshesClassWhenRemoteDefinitionChanges() throws Exception {
        remoteFunction.set(functionFor(FirstTableFunction.class));
        assertEquals(
                FirstTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun")
        );

        remoteFunction.set(functionFor(SecondTableFunction.class));
        assertEquals(
                FirstTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun"),
                "definition should remain cached before its refresh interval"
        );

        refreshDefinition("default", "remote_fun");
        awaitFunctionClass(SecondTableFunction.class);
    }

    @Test
    void refreshesDeletedAndRecreatedDefinitions() throws Exception {
        remoteFunction.set(functionFor(FirstTableFunction.class));
        assertEquals(
                FirstTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun")
        );

        remoteFunction.set(null);
        invalidateDefinition("default", "remote_fun");
        assertNull(JavaFunctionUtils.getTableFunctionClass("default", "remote_fun"));

        remoteFunction.set(functionFor(SecondTableFunction.class));
        assertNull(
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun"),
                "a missing definition should also be cached"
        );

        invalidateDefinition("default", "remote_fun");
        assertEquals(
                SecondTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun")
        );
    }

    @Test
    void normalizesRemoteAndRegisteredFunctionNames() throws Exception {
        remoteFunction.set(functionFor(FirstTableFunction.class));
        assertEquals(
                FirstTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass(" DEFAULT ", " ReMoTe_FuN ")
        );

        JavaFunctionUtils.registerTableFunction(" DEFAULT ", " LoCaL_FuN ", SecondTableFunction.class);
        assertEquals(
                SecondTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "local_fun")
        );
    }

    private static Function functionFor(Class<?> clazz) {
        Function function = new Function();
        function.setDbName("default");
        function.setFunctionName("remote_fun");
        function.setClassName(clazz.getName());
        return function;
    }

    private static void refreshDefinition(String db, String functionName) throws Exception {
        getJavaFunctionClassCache().refresh(db + "." + functionName);
    }

    private static void awaitFunctionClass(Class<?> expectedClass) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (expectedClass.equals(JavaFunctionUtils.getTableFunctionClass("default", "remote_fun"))) {
                return;
            }
            Thread.sleep(10);
        }
        assertEquals(
                expectedClass,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun")
        );
    }

    private static void invalidateDefinition(String db, String functionName) throws Exception {
        getJavaFunctionClassCache().invalidate(db + "." + functionName);
    }

    @SuppressWarnings("unchecked")
    private static LoadingCache<String, Optional<Class<?>>> getJavaFunctionClassCache() throws Exception {
        Field cacheField = JavaFunctionUtils.class.getDeclaredField("javaFunctionClassCache");
        cacheField.setAccessible(true);
        return (LoadingCache<String, Optional<Class<?>>>) cacheField.get(null);
    }

    private static Field getMetadataAccessField() throws Exception {
        Field field = MetadataAccessFactory.class.getDeclaredField("instance");
        field.setAccessible(true);
        return field;
    }

    public static class FirstTableFunction {
    }

    public static class SecondTableFunction {
    }

    private static final class MutableFunctionSchemaAccess implements SchemaAccess {
        private final AtomicReference<Function> remoteFunction;

        private MutableFunctionSchemaAccess(AtomicReference<Function> remoteFunction) {
            this.remoteFunction = remoteFunction;
        }

        @Override
        public List<String> getDatabases() {
            return Collections.singletonList("default");
        }

        @Override
        public List<Table> getTables(String database) {
            return Collections.emptyList();
        }

        @Override
        public Table getTable(String database, String tableName) throws NoSuchObjectException {
            throw new NoSuchObjectException("table not found");
        }

        @Override
        public List<Function> getFunctions(String database) {
            Function function = remoteFunction.get();
            return function == null ? Collections.emptyList() : Collections.singletonList(function);
        }

        @Override
        public Function getFunction(String database, String funName) throws NoSuchObjectException {
            if (!"default".equals(database) || !"remote_fun".equals(funName)) {
                throw new NoSuchObjectException("function not found");
            }
            Function function = remoteFunction.get();
            if (function == null) {
                throw new NoSuchObjectException("function not found");
            }
            return function;
        }

        @Override
        public long getTableUpdateTime(String database, String table) {
            return 0;
        }

        @Override
        public List<String> getPartitionPaths(String database, String table, String partitionFilter) {
            return Collections.emptyList();
        }
    }
}
