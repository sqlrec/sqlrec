package com.sqlrec.db.remote;

import org.apache.calcite.sql.*;
import org.apache.flink.sql.parser.ddl.*;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.types.DataType;

import java.util.*;

import static org.apache.flink.sql.parser.ddl.SqlTableLike.FeatureOption;
import static org.apache.flink.sql.parser.ddl.SqlTableLike.MergingStrategy;

/** Converts in-memory metadata definitions. HiveCatalog owns HMS I/O and serialization.
 * Computed columns and watermarks require a planner and are deliberately unsupported. */
final class FlinkTableDdl {
    private FlinkTableDdl() {}

    static ResolvedCatalogTable create(SqlCreateTable sql, DataTypeFactory types) throws Exception {
        if (sql instanceof SqlCreateTableLike) throw new IllegalArgumentException("CREATE TABLE LIKE requires a source table");
        requireSimpleSchema(sql.getColumnList(), sql.getWatermark().isPresent());
        sql.validate();
        TableDefinition definition = new TableDefinition();
        mergeDeclaredDefinition(definition, sql, defaultStrategies(), types);
        return definition.toCatalogTable();
    }

    static ResolvedCatalogTable createLike(SqlCreateTableLike sql, ResolvedCatalogTable source,
                                           DataTypeFactory types) throws Exception {
        Objects.requireNonNull(source, "LIKE source table");
        requireSimpleSchema(sql.getColumnList(), sql.getWatermark().isPresent());
        sql.validate();
        Map<FeatureOption, MergingStrategy> strategies = likeStrategies(sql.getTableLike());
        TableDefinition definition = new TableDefinition();
        inheritSource(definition, source, strategies);
        mergeDeclaredDefinition(definition, sql, strategies, types);
        return definition.toCatalogTable();
    }

    private static Map<FeatureOption, MergingStrategy> defaultStrategies() {
        Map<FeatureOption, MergingStrategy> strategies = new EnumMap<>(FeatureOption.class);
        for (FeatureOption feature : FeatureOption.values()) strategies.put(feature, MergingStrategy.INCLUDING);
        strategies.put(FeatureOption.OPTIONS, MergingStrategy.OVERWRITING);
        return strategies;
    }

    private static Map<FeatureOption, MergingStrategy> likeStrategies(SqlTableLike clause) {
        Map<FeatureOption, MergingStrategy> strategies = defaultStrategies();
        for (var option : clause.getOptions()) {
            if (option.getFeatureOption() == FeatureOption.ALL) {
                for (FeatureOption feature : FeatureOption.values()) strategies.put(feature, option.getMergingStrategy());
            }
        }
        for (var option : clause.getOptions()) strategies.put(option.getFeatureOption(), option.getMergingStrategy());
        return strategies;
    }

    private static void inheritSource(TableDefinition definition, ResolvedCatalogTable source,
                                      Map<FeatureOption, MergingStrategy> strategies) {
        for (Column column : source.getResolvedSchema().getColumns()) {
            if (!(column instanceof Column.MetadataColumn) || strategies.get(FeatureOption.METADATA) != MergingStrategy.EXCLUDING) {
                definition.columns.add(column);
            }
        }
        if (strategies.get(FeatureOption.CONSTRAINTS) != MergingStrategy.EXCLUDING) {
            definition.primaryKey = source.getResolvedSchema().getPrimaryKey().orElse(null);
        }
        // Flink 1.19 keeps source partitions unless the CREATE declares replacements,
        // even for EXCLUDING PARTITIONS / ALL.
        definition.partitions.addAll(source.getPartitionKeys());
        if (strategies.get(FeatureOption.OPTIONS) != MergingStrategy.EXCLUDING) definition.options.putAll(source.getOptions());
    }

    private static void mergeDeclaredDefinition(TableDefinition definition, SqlCreateTable sql,
                                                Map<FeatureOption, MergingStrategy> strategies, DataTypeFactory types) {
        for (SqlNode node : sql.getColumnList()) {
            Column column = column((SqlTableColumn) node, types);
            int previous = indexOf(definition.columns, column.getName());
            if (previous >= 0) {
                if (!(column instanceof Column.MetadataColumn) || !(definition.columns.get(previous) instanceof Column.MetadataColumn)
                        || strategies.get(FeatureOption.METADATA) != MergingStrategy.OVERWRITING) {
                    throw invalid("Duplicate column: " + column.getName());
                }
                definition.columns.set(previous, column);
            } else definition.columns.add(column);
        }
        if (!sql.getFullConstraints().isEmpty()) {
            if (definition.primaryKey != null) throw invalid("A primary key is already defined in the LIKE source");
            definition.primaryKey = primaryKey(sql.getFullConstraints().get(0));
        }
        List<String> declaredPartitions = identifiers(sql.getPartitionKeyList());
        if (!declaredPartitions.isEmpty()) {
            if (!definition.partitions.isEmpty() && strategies.get(FeatureOption.PARTITIONS) != MergingStrategy.EXCLUDING) {
                throw invalid("Partition keys are already defined in the LIKE source");
            }
            definition.partitions.clear();
            definition.partitions.addAll(declaredPartitions);
        }
        Map<String, String> declaredOptions = properties(sql.getPropertyList());
        if (strategies.get(FeatureOption.OPTIONS) == MergingStrategy.INCLUDING) {
            for (String key : declaredOptions.keySet()) {
                if (definition.options.containsKey(key)) throw invalid("Duplicate LIKE option: " + key);
            }
        }
        definition.options.putAll(declaredOptions);
        definition.comment = sql.getComment().map(FlinkTableDdl::literal).orElse(null);
    }

    static ResolvedCatalogTable resolve(CatalogBaseTable definition, DataTypeFactory types) {
        if (!(definition instanceof CatalogTable table)) throw invalid("The object is not a table");
        Schema schema = table.getUnresolvedSchema();
        if (!schema.getWatermarkSpecs().isEmpty()) throw unsupported();
        TableDefinition resolved = new TableDefinition();
        for (Schema.UnresolvedColumn unresolved : schema.getColumns()) {
            Column column;
            if (unresolved instanceof Schema.UnresolvedPhysicalColumn physical) {
                column = Column.physical(physical.getName(), types.createDataType(physical.getDataType()));
            } else if (unresolved instanceof Schema.UnresolvedMetadataColumn metadata) {
                column = Column.metadata(metadata.getName(), types.createDataType(metadata.getDataType()),
                        metadata.getMetadataKey(), metadata.isVirtual());
            } else throw unsupported();
            resolved.columns.add(column.withComment(unresolved.getComment().orElse(null)));
        }
        resolved.primaryKey = schema.getPrimaryKey().map(key -> UniqueConstraint.primaryKey(
                key.getConstraintName(), key.getColumnNames())).orElse(null);
        resolved.partitions.addAll(table.getPartitionKeys());
        resolved.options.putAll(table.getOptions());
        resolved.comment = table.getComment();
        resolved.snapshot = table.getSnapshot().orElse(null);
        return resolved.toCatalogTable();
    }

    static ResolvedCatalogTable alter(SqlAlterTable sql, CatalogBaseTable previous, DataTypeFactory types) throws Exception {
        ResolvedCatalogTable old = resolve(previous, types);
        if (sql instanceof SqlAlterTableOptions set) {
            Map<String, String> options = new HashMap<>(old.getOptions());
            options.putAll(properties(set.getPropertyList()));
            return old.copy(options);
        }
        if (sql instanceof SqlAlterTableReset reset) {
            if (reset.getResetKeys().isEmpty() || reset.getResetKeys().contains("connector")) {
                throw invalid("ALTER TABLE RESET requires keys and cannot reset connector");
            }
            Map<String, String> options = new HashMap<>(old.getOptions());
            reset.getResetKeys().forEach(options::remove);
            return old.copy(options);
        }
        return alterSchema(sql, old, types);
    }

    private static ResolvedCatalogTable alterSchema(SqlAlterTable sql, ResolvedCatalogTable old, DataTypeFactory types) throws Exception {
        TableDefinition definition = new TableDefinition(old);
        if (sql instanceof SqlAlterTableSchema change) {
            requireSimpleSchema(change.getColumnPositions().getList().stream()
                    .map(position -> ((SqlTableColumnPosition) position).getColumn()).toList(), change.getWatermark().isPresent());
            change.validate();
            changeColumns(definition.columns, change, types);
            if (change.getFullConstraint().isPresent()) {
                if (change instanceof SqlAlterTableAdd && definition.primaryKey != null) throw invalid("Primary key already exists");
                if (change instanceof SqlAlterTableModify && definition.primaryKey == null) throw invalid("Primary key does not exist");
                definition.primaryKey = primaryKey(change.getFullConstraint().orElseThrow());
            }
        } else if (sql instanceof SqlAlterTableDropColumn drop) {
            for (String name : identifiers(drop.getColumnList())) {
                definition.columns.remove(requireColumn(definition.columns, name));
            }
        } else if (sql instanceof SqlAlterTableRenameColumn rename) {
            renameColumn(definition, rename);
        } else if (sql instanceof SqlAlterTableDropPrimaryKey || sql instanceof SqlAlterTableDropConstraint) {
            if (definition.primaryKey == null) throw invalid("Primary key does not exist");
            if (sql instanceof SqlAlterTableDropConstraint drop
                    && !definition.primaryKey.getName().equals(drop.getConstraintName().getSimple())) {
                throw invalid("Constraint does not exist: " + drop.getConstraintName().getSimple());
            }
            definition.primaryKey = null;
        } else {
            throw new UnsupportedOperationException("Unsupported table metadata alteration: " + sql.getClass().getSimpleName());
        }
        return definition.toCatalogTable();
    }

    private static void changeColumns(List<Column> columns, SqlAlterTableSchema change, DataTypeFactory types) {
        boolean add = change instanceof SqlAlterTableAdd;
        for (SqlNode node : change.getColumnPositions()) {
            var position = (SqlTableColumnPosition) node;
            Column column = column(position.getColumn(), types);
            int existing = indexOf(columns, column.getName());
            if (add && existing >= 0) throw invalid("Column already exists: " + column.getName());
            if (!add && existing < 0) throw invalid("Column does not exist: " + column.getName());
            if (!add) columns.remove(existing);
            int insertion = add ? columns.size() : existing;
            if (position.isFirstColumn()) insertion = 0;
            else if (position.isAfterReferencedColumn()) {
                insertion = requireColumn(columns, position.getAfterReferencedColumn().getSimple()) + 1;
            }
            columns.add(insertion, column);
        }
    }

    private static void renameColumn(TableDefinition definition, SqlAlterTableRenameColumn rename) {
        String name = rename.getOldColumnIdentifier().getSimple();
        String replacement = rename.getNewColumnIdentifier().getSimple();
        int index = requireColumn(definition.columns, name);
        if (indexOf(definition.columns, replacement) >= 0) throw invalid("Column already exists: " + replacement);
        Column previous = definition.columns.get(index);
        Column renamed = previous instanceof Column.MetadataColumn metadata
                ? Column.metadata(replacement, metadata.getDataType(), metadata.getMetadataKey().orElse(null), metadata.isVirtual())
                : Column.physical(replacement, previous.getDataType());
        definition.columns.set(index, renamed.withComment(previous.getComment().orElse(null)));
        definition.partitions.replaceAll(key -> key.equals(name) ? replacement : key);
        if (definition.primaryKey != null) {
            definition.primaryKey = UniqueConstraint.primaryKey(definition.primaryKey.getName(),
                    definition.primaryKey.getColumns().stream().map(key -> key.equals(name) ? replacement : key).toList());
        }
    }

    private static Column column(SqlTableColumn sql, DataTypeFactory types) {
        Column column;
        if (sql instanceof SqlTableColumn.SqlRegularColumn physical) {
            column = Column.physical(sql.getName().getSimple(), dataType(physical.getType(), types));
        } else if (sql instanceof SqlTableColumn.SqlMetadataColumn metadata) {
            column = Column.metadata(sql.getName().getSimple(), dataType(metadata.getType(), types),
                    metadata.getMetadataAlias().orElse(null), metadata.isVirtual());
        } else throw unsupported();
        return column.withComment(sql.getComment().map(FlinkTableDdl::literal).orElse(null));
    }

    private static DataType dataType(SqlDataTypeSpec sql, DataTypeFactory types) {
        // The SQL parser carries top-level nullability separately from the type's SQL text.
        DataType type = types.createDataType(sql.toString());
        return Boolean.FALSE.equals(sql.getNullable()) ? type.notNull() : type.nullable();
    }

    private static UniqueConstraint primaryKey(SqlTableConstraint sql) {
        if (!sql.isPrimaryKey() || sql.isEnforced()) throw invalid("Only PRIMARY KEY NOT ENFORCED is supported");
        List<String> names = Arrays.asList(sql.getColumnNames());
        return UniqueConstraint.primaryKey(sql.getConstraintName().orElse("PK_" + String.join("_", names)), names);
    }

    /** Mutable only during one conversion; no source table collections are modified. */
    private static final class TableDefinition {
        private final List<Column> columns = new ArrayList<>();
        private final List<String> partitions = new ArrayList<>();
        private final Map<String, String> options = new HashMap<>();
        private UniqueConstraint primaryKey;
        private String comment;
        private Long snapshot;

        private TableDefinition() {}

        private TableDefinition(ResolvedCatalogTable table) {
            columns.addAll(table.getResolvedSchema().getColumns());
            partitions.addAll(table.getPartitionKeys());
            options.putAll(table.getOptions());
            primaryKey = table.getResolvedSchema().getPrimaryKey().orElse(null);
            comment = table.getComment();
            snapshot = table.getSnapshot().orElse(null);
        }

        private ResolvedCatalogTable toCatalogTable() {
            validateSchema(columns, primaryKey, partitions);
            ResolvedSchema resolved = new ResolvedSchema(columns, List.of(), primaryKey);
            Schema schema = Schema.newBuilder().fromResolvedSchema(resolved).build();
            return new ResolvedCatalogTable(CatalogTable.of(schema, comment, partitions, options, snapshot), resolved);
        }
    }

    private static void validateSchema(List<Column> columns, UniqueConstraint primaryKey, List<String> partitions) {
        Set<String> names = new HashSet<>();
        Set<String> metadataKeys = new HashSet<>();
        for (Column column : columns) {
            if (!names.add(column.getName())) throw invalid("Duplicate column: " + column.getName());
            if (column instanceof Column.MetadataColumn metadata
                    && !metadataKeys.add(metadata.getMetadataKey().orElse(metadata.getName()))) {
                throw invalid("Duplicate metadata key: " + metadata.getMetadataKey().orElse(metadata.getName()));
            }
        }
        if (primaryKey != null) {
            if (primaryKey.getColumns().isEmpty() || new HashSet<>(primaryKey.getColumns()).size() != primaryKey.getColumns().size()) {
                throw invalid("Primary key must contain distinct columns");
            }
            for (String key : primaryKey.getColumns()) {
                Column column = columns.get(requireColumn(columns, key));
                if (!column.isPhysical() || column.getDataType().getLogicalType().isNullable()) {
                    throw invalid("Primary key column must be physical and NOT NULL: " + key);
                }
            }
        }
        if (new HashSet<>(partitions).size() != partitions.size()) throw invalid("Duplicate partition keys");
        for (String key : partitions) {
            if (!columns.get(requireColumn(columns, key)).isPhysical()) throw invalid("Partition key must be a physical column: " + key);
        }
    }

    static void requireSimpleSchema(Iterable<? extends SqlNode> columns, boolean watermark) {
        if (watermark) throw unsupported();
        for (SqlNode column : columns) if (column instanceof SqlTableColumn.SqlComputedColumn) throw unsupported();
    }

    static Map<String, String> properties(SqlNodeList nodes) {
        Map<String, String> properties = new HashMap<>();
        if (nodes == null) return properties; // ADD PARTITION may omit WITH properties.
        for (SqlNode node : nodes) {
            var option = (SqlTableOption) node;
            properties.put(option.getKeyString(), option.getValueString());
        }
        return properties;
    }

    private static List<String> identifiers(SqlNodeList nodes) {
        return nodes.getList().stream().map(SqlIdentifier.class::cast).map(SqlIdentifier::getSimple).toList();
    }

    private static String literal(SqlNode node) { return ((SqlLiteral) node).getValueAs(String.class); }

    private static int indexOf(List<Column> columns, String name) {
        for (int i = 0; i < columns.size(); i++) if (columns.get(i).getName().equals(name)) return i;
        return -1;
    }

    private static int requireColumn(List<Column> columns, String name) {
        int index = indexOf(columns, name);
        if (index < 0) throw invalid("Column does not exist: " + name);
        return index;
    }

    private static ValidationException invalid(String message) { return new ValidationException(message); }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("UNSUPPORTED_METADATA_DDL: computed columns and WATERMARK are not supported");
    }
}
