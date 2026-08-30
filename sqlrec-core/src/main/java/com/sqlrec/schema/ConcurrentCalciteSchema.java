package com.sqlrec.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.NameSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A {@link CalciteSchema} whose dynamically added tables, types and sub-schemas
 * are safe to read while other threads are adding entries.
 *
 * <p>Calcite's own explicit-object maps are deliberately left empty. Dynamic
 * entries are stored in concurrent maps and exposed through the implicit-object
 * hooks used by the final lookup methods in {@code CalciteSchema}. The wrapped
 * user {@link Schema}, for example {@link HmsSchema}, remains the source of
 * metadata-backed implicit objects.</p>
 *
 * <p>This class does not make planners, validators, tables, or functions
 * generally thread-safe. Functions and lattices must be registered before the
 * schema is used concurrently. SQLRec only mutates tables during parallel
 * execution.</p>
 */
public final class ConcurrentCalciteSchema extends CalciteSchema {
    private final ConcurrentNavigableMap<String, TableEntry> concurrentTableMap =
            new ConcurrentSkipListMap<>(NameSet.COMPARATOR);
    private final ConcurrentNavigableMap<String, TypeEntry> concurrentTypeMap =
            new ConcurrentSkipListMap<>(NameSet.COMPARATOR);
    private final ConcurrentNavigableMap<String, ConcurrentCalciteSchema> concurrentSubSchemaMap =
            new ConcurrentSkipListMap<>(NameSet.COMPARATOR);

    private ConcurrentCalciteSchema(
            @Nullable CalciteSchema parent,
            Schema schema,
            String name,
            @Nullable List<? extends List<String>> path) {
        // Leave CalciteSchema's NameMap instances empty. All mutable entries
        // owned by SQLRec are stored in the concurrent maps above.
        super(parent, schema, name,
                null, null, null, null, null, null, null, path);
    }

    /** Creates an empty root schema without Calcite's metadata schema. */
    public static ConcurrentCalciteSchema createRootSchema() {
        return new ConcurrentCalciteSchema(null, new RootSchema(), "", null);
    }

    @Override
    public TableEntry add(String tableName, Table table) {
        return add(tableName, table, ImmutableList.of());
    }

    @Override
    public TableEntry add(String tableName, Table table, ImmutableList<String> sqls) {
        TableEntry entry = new TableEntryImpl(this, tableName, table, sqls);
        concurrentTableMap.put(tableName, entry);
        return entry;
    }

    @Override
    public TypeEntry add(String name, RelProtoDataType type) {
        TypeEntry entry = new TypeEntryImpl(this, name, type);
        concurrentTypeMap.put(name, entry);
        return entry;
    }

    @Override
    public CalciteSchema add(String name, Schema schema) {
        ConcurrentCalciteSchema child = new ConcurrentCalciteSchema(this, schema, name, null);
        concurrentSubSchemaMap.put(name, child);
        return child;
    }

    @Override
    public boolean removeTable(String name) {
        return concurrentTableMap.remove(name) != null;
    }

    @Override
    public boolean removeType(String name) {
        return concurrentTypeMap.remove(name) != null;
    }

    @Override
    public boolean removeSubSchema(String name) {
        return concurrentSubSchemaMap.remove(name) != null;
    }

    @Override
    protected @Nullable CalciteSchema getImplicitSubSchema(
            String schemaName, boolean caseSensitive) {
        ConcurrentCalciteSchema explicit = lookup(
                concurrentSubSchemaMap, schemaName, caseSensitive);
        if (explicit != null) {
            return explicit;
        }

        String actualName = lookupName(
                schema.getSubSchemaNames(), schemaName, caseSensitive);
        if (actualName == null) {
            return null;
        }
        Schema childSchema = schema.getSubSchema(actualName);
        if (childSchema == null) {
            return null;
        }
        return concurrentSubSchemaMap.computeIfAbsent(actualName,
                ignored -> new ConcurrentCalciteSchema(this, childSchema, actualName, null));
    }

    @Override
    protected @Nullable TableEntry getImplicitTable(
            String tableName, boolean caseSensitive) {
        TableEntry explicit = lookup(concurrentTableMap, tableName, caseSensitive);
        if (explicit != null) {
            return explicit;
        }

        String actualName = lookupName(schema.getTableNames(), tableName, caseSensitive);
        if (actualName == null) {
            return null;
        }
        Table table = schema.getTable(actualName);
        return table == null ? null : tableEntry(actualName, table);
    }

    @Override
    protected @Nullable TypeEntry getImplicitType(String name, boolean caseSensitive) {
        TypeEntry explicit = lookup(concurrentTypeMap, name, caseSensitive);
        if (explicit != null) {
            return explicit;
        }

        String actualName = lookupName(schema.getTypeNames(), name, caseSensitive);
        if (actualName == null) {
            return null;
        }
        RelProtoDataType type = schema.getType(actualName);
        return type == null ? null : typeEntry(actualName, type);
    }

    @Override
    protected @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(
            String tableName, boolean caseSensitive) {
        String actualName = lookupName(
                schema.getFunctionNames(), tableName, caseSensitive);
        if (actualName == null) {
            return null;
        }
        Collection<Function> functions = schema.getFunctions(actualName);
        if (functions != null) {
            for (Function function : functions) {
                if (function instanceof TableMacro && function.getParameters().isEmpty()) {
                    Table table = ((TableMacro) function).apply(ImmutableList.of());
                    return tableEntry(actualName, table);
                }
            }
        }
        return null;
    }

    @Override
    protected void addImplicitSubSchemaToBuilder(
            ImmutableSortedMap.Builder<String, CalciteSchema> builder) {
        builder.putAll(concurrentSubSchemaMap);
        for (String schemaName : schema.getSubSchemaNames()) {
            if (concurrentSubSchemaMap.containsKey(schemaName)) {
                continue;
            }
            Schema childSchema = schema.getSubSchema(schemaName);
            if (childSchema != null) {
                ConcurrentCalciteSchema child = concurrentSubSchemaMap.computeIfAbsent(
                        schemaName,
                        ignored -> new ConcurrentCalciteSchema(this, childSchema, schemaName, null));
                builder.put(schemaName, child);
            }
        }
    }

    @Override
    protected void addImplicitTableToBuilder(
            ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(concurrentTableMap.keySet());
        for (String tableName : schema.getTableNames()) {
            if (!concurrentTableMap.containsKey(tableName)) {
                builder.add(tableName);
            }
        }
    }

    @Override
    protected void addImplicitFunctionsToBuilder(
            ImmutableList.Builder<Function> builder,
            String name,
            boolean caseSensitive) {
        String actualName = lookupName(schema.getFunctionNames(), name, caseSensitive);
        if (actualName == null) {
            return;
        }
        Collection<Function> functions = schema.getFunctions(actualName);
        if (functions != null) {
            builder.addAll(functions);
        }
    }

    @Override
    protected void addImplicitFuncNamesToBuilder(
            ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(schema.getFunctionNames());
    }

    @Override
    protected void addImplicitTypeNamesToBuilder(
            ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(concurrentTypeMap.keySet());
        for (String typeName : schema.getTypeNames()) {
            if (!concurrentTypeMap.containsKey(typeName)) {
                builder.add(typeName);
            }
        }
    }

    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
            ImmutableSortedMap.Builder<String, Table> builder) {
        ImmutableSortedMap<String, Table> explicitTables = builder.build();
        for (String functionName : schema.getFunctionNames()) {
            if (explicitTables.containsKey(functionName)) {
                continue;
            }
            for (Function function : schema.getFunctions(functionName)) {
                if (function instanceof TableMacro && function.getParameters().isEmpty()) {
                    builder.put(functionName,
                            ((TableMacro) function).apply(ImmutableList.of()));
                    break;
                }
            }
        }
    }

    @Override
    protected CalciteSchema snapshot(
            @Nullable CalciteSchema parent, SchemaVersion version) {
        ConcurrentCalciteSchema snapshot = new ConcurrentCalciteSchema(
                parent, schema.snapshot(version), name, getPath());

        concurrentTableMap.forEach((tableName, entry) ->
                snapshot.concurrentTableMap.put(tableName,
                        new TableEntryImpl(snapshot, tableName, entry.getTable(), entry.sqls)));
        concurrentTypeMap.forEach((typeName, entry) ->
                snapshot.concurrentTypeMap.put(typeName,
                        new TypeEntryImpl(snapshot, typeName, entry.getType())));
        concurrentSubSchemaMap.forEach((schemaName, child) ->
                snapshot.concurrentSubSchemaMap.put(schemaName,
                        (ConcurrentCalciteSchema) child.snapshot(snapshot, version)));
        return snapshot;
    }

    @Override
    public void setCache(boolean cache) {
        if (cache) {
            throw new UnsupportedOperationException(
                    "ConcurrentCalciteSchema does not cache implicit object names");
        }
    }

    @Override
    protected boolean isCacheEnabled() {
        return false;
    }

    private static <V> @Nullable V lookup(
            NavigableMap<String, V> map, String name, boolean caseSensitive) {
        String actualName = lookupName(map.navigableKeySet(), name, caseSensitive);
        return actualName == null ? null : map.get(actualName);
    }

    private static @Nullable String lookupName(
            Set<String> candidates, String name, boolean caseSensitive) {
        if (candidates.contains(name)) {
            return name;
        }
        if (caseSensitive) {
            return null;
        }

        String upperName = name.toUpperCase(Locale.ROOT);
        if (candidates.contains(upperName)) {
            return upperName;
        }
        String lowerName = name.toLowerCase(Locale.ROOT);
        if (candidates.contains(lowerName)) {
            return lowerName;
        }
        for (String candidate : candidates) {
            if (candidate.equalsIgnoreCase(name)) {
                return candidate;
            }
        }
        return null;
    }

    /** Root schema expression equivalent to Calcite's package-private RootSchema. */
    private static final class RootSchema extends AbstractSchema {
        @Override
        public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
            return Expressions.call(
                    DataContext.ROOT,
                    BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
        }
    }
}
