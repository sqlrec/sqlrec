package com.sqlrec.schema;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
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
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class JavaFunctionUtilsTest {
    private MetadataAccess savedMetadataAccess;
    private final AtomicReference<Function> remoteFunction = new AtomicReference<>();
    private ManualTicker ticker;
    private QueuedExecutor executor;
    private LoadingCache<String, Optional<Class<?>>> cache;

    @BeforeEach
    void setUp() throws Exception {
        Field instanceField = getMetadataAccessField();
        savedMetadataAccess = (MetadataAccess) instanceField.get(null);
        instanceField.set(null, new MetadataAccess(
                new MutableFunctionSchemaAccess(remoteFunction),
                new InMemoryStoreAccess(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
                new LocalHdfsAccess()
        ));

        ticker = new ManualTicker();
        executor = new QueuedExecutor();
        cache = JavaFunctionUtils.createCache(Duration.ofSeconds(1), executor, ticker);
        JavaFunctionUtils.setSkipHmsQuery(false);
    }

    @AfterEach
    void tearDown() throws Exception {
        JavaFunctionUtils.unregisterTableFunction("default", "local_fun");
        JavaFunctionUtils.setSkipHmsQuery(false);
        getMetadataAccessField().set(null, savedMetadataAccess);
    }

    @Test
    void refreshesClassWhenRemoteDefinitionChanges() throws Exception {
        remoteFunction.set(functionFor(FirstTableFunction.class));
        assertEquals(
                FirstTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun", cache)
        );

        remoteFunction.set(functionFor(SecondTableFunction.class));
        assertEquals(
                FirstTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun", cache),
                "definition should remain cached before its refresh interval"
        );

        ticker.advance(Duration.ofMillis(1100));
        assertEquals(
                FirstTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun", cache),
                "definition should remain cached until the queued refresh runs"
        );
        executor.runAll();
        assertEquals(
                SecondTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun", cache)
        );
    }

    @Test
    void refreshesDeletedAndRecreatedDefinitions() throws Exception {
        remoteFunction.set(functionFor(FirstTableFunction.class));
        assertEquals(
                FirstTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun", cache)
        );

        remoteFunction.set(null);
        cache.invalidate("default.remote_fun");
        assertNull(JavaFunctionUtils.getTableFunctionClass("default", "remote_fun", cache));

        remoteFunction.set(functionFor(SecondTableFunction.class));
        assertNull(
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun", cache),
                "a missing definition should also be cached"
        );

        cache.invalidate("default.remote_fun");
        assertEquals(
                SecondTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "remote_fun", cache)
        );
    }

    @Test
    void normalizesRemoteAndRegisteredFunctionNames() throws Exception {
        remoteFunction.set(functionFor(FirstTableFunction.class));
        assertEquals(
                FirstTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass(" DEFAULT ", " ReMoTe_FuN ", cache)
        );

        JavaFunctionUtils.registerTableFunction(" DEFAULT ", " LoCaL_FuN ", SecondTableFunction.class);
        assertEquals(
                SecondTableFunction.class,
                JavaFunctionUtils.getTableFunctionClass("default", "local_fun", cache)
        );
    }

    private static Function functionFor(Class<?> clazz) {
        Function function = new Function();
        function.setDbName("default");
        function.setFunctionName("remote_fun");
        function.setClassName(clazz.getName());
        return function;
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

    private static final class ManualTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        void runAll() {
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }
        }
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
