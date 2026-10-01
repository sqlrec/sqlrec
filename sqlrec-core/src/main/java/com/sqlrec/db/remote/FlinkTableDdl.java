package com.sqlrec.db.remote;

import org.apache.calcite.sql.*;
import org.apache.flink.sql.parser.ddl.*;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.types.DataType;

import java.util.*;

import static org.apache.flink.sql.parser.ddl.SqlTableLike.FeatureOption;
import static org.apache.flink.sql.parser.ddl.SqlTableLike.MergingStrategy;

/** Converts metadata-only table DDL. HiveCatalog remains responsible for HMS representation.
 * Computed columns and watermarks require a planner and are deliberately unsupported. */
final class FlinkTableDdl {
    private FlinkTableDdl() {}

    static ResolvedCatalogTable create(SqlCreateTable sql, HiveCatalog catalog, DataTypeFactory types,
                                       String database) throws Exception {
        sql.validate();
        List<Column> columns = new ArrayList<>();
        List<String> partitions = new ArrayList<>();
        UniqueConstraint primaryKey = null;
        Map<String, String> options = new HashMap<>();
        Map<FeatureOption, MergingStrategy> strategies = new EnumMap<>(FeatureOption.class);
        for (FeatureOption feature : FeatureOption.values()) strategies.put(feature, MergingStrategy.INCLUDING);
        strategies.put(FeatureOption.OPTIONS, MergingStrategy.OVERWRITING);
        if (sql instanceof SqlCreateTableLike like) {
            SqlTableLike clause = like.getTableLike();
            for (var option : clause.getOptions()) {
                if (option.getFeatureOption() == FeatureOption.ALL) {
                    for (FeatureOption feature : FeatureOption.values()) strategies.put(feature, option.getMergingStrategy());
                }
            }
            for (var option : clause.getOptions()) strategies.put(option.getFeatureOption(), option.getMergingStrategy());
            ObjectPath source = FlinkHiveDdlAdapter.objectPath(clause.getSourceTable().names.toArray(String[]::new), database);
            ResolvedCatalogTable inherited = resolve(catalog.getTable(source), types);
            for (Column column : inherited.getResolvedSchema().getColumns()) {
                if (!(column instanceof Column.MetadataColumn) || strategies.get(FeatureOption.METADATA) != MergingStrategy.EXCLUDING) {
                    columns.add(column);
                }
            }
            if (strategies.get(FeatureOption.CONSTRAINTS) != MergingStrategy.EXCLUDING) {
                primaryKey = inherited.getResolvedSchema().getPrimaryKey().orElse(null);
            }
            if (strategies.get(FeatureOption.PARTITIONS) != MergingStrategy.EXCLUDING) partitions.addAll(inherited.getPartitionKeys());
            if (strategies.get(FeatureOption.OPTIONS) != MergingStrategy.EXCLUDING) options.putAll(inherited.getOptions());
        }
        for (SqlNode node : sql.getColumnList()) {
            Column column = column((SqlTableColumn) node, types);
            int previous = indexOf(columns, column.getName());
            if (previous >= 0) {
                if (!(column instanceof Column.MetadataColumn) || !(columns.get(previous) instanceof Column.MetadataColumn)
                        || strategies.get(FeatureOption.METADATA) != MergingStrategy.OVERWRITING) {
                    throw invalid("Duplicate column: " + column.getName());
                }
                columns.set(previous, column);
            } else columns.add(column);
        }
        if (!sql.getFullConstraints().isEmpty()) {
            if (primaryKey != null) throw invalid("A primary key is already defined in the LIKE source");
            primaryKey = primaryKey(sql.getFullConstraints().get(0));
        }
        List<String> declaredPartitions = identifiers(sql.getPartitionKeyList());
        if (!declaredPartitions.isEmpty()) {
            if (!partitions.isEmpty()) throw invalid("Partition keys are already defined in the LIKE source");
            partitions = declaredPartitions;
        }
        Map<String, String> declaredOptions = properties(sql.getPropertyList());
        if (strategies.get(FeatureOption.OPTIONS) == MergingStrategy.INCLUDING) {
            for (String key : declaredOptions.keySet()) {
                if (options.containsKey(key)) throw invalid("Duplicate LIKE option: " + key);
            }
        }
        options.putAll(declaredOptions);
        String comment = sql.getComment().map(FlinkHiveDdlAdapter::literal).orElse(null);
        return table(columns, primaryKey, partitions, options, comment, null);
    }

    static ResolvedCatalogTable resolve(CatalogBaseTable definition, DataTypeFactory types) {
        if (!(definition instanceof CatalogTable table)) throw invalid("The object is not a table");
        Schema schema = table.getUnresolvedSchema();
        if (!schema.getWatermarkSpecs().isEmpty()) throw unsupported();
        List<Column> columns = new ArrayList<>();
        for (Schema.UnresolvedColumn unresolved : schema.getColumns()) {
            Column column;
            if (unresolved instanceof Schema.UnresolvedPhysicalColumn physical) {
                column = Column.physical(physical.getName(), types.createDataType(physical.getDataType()));
            } else if (unresolved instanceof Schema.UnresolvedMetadataColumn metadata) {
                column = Column.metadata(metadata.getName(), types.createDataType(metadata.getDataType()),
                        metadata.getMetadataKey(), metadata.isVirtual());
            } else throw unsupported();
            columns.add(column.withComment(unresolved.getComment().orElse(null)));
        }
        UniqueConstraint primaryKey = schema.getPrimaryKey().map(key -> UniqueConstraint.primaryKey(
                key.getConstraintName(), key.getColumnNames())).orElse(null);
        return table(columns, primaryKey, table.getPartitionKeys(), table.getOptions(), table.getComment(),
                table.getSnapshot().orElse(null));
    }

    static ResolvedCatalogTable alter(SqlAlterTable sql, CatalogBaseTable previous, DataTypeFactory types) throws Exception {
        ResolvedCatalogTable old = resolve(previous, types);
        List<Column> columns = new ArrayList<>(old.getResolvedSchema().getColumns());
        List<String> partitions = new ArrayList<>(old.getPartitionKeys());
        UniqueConstraint primaryKey = old.getResolvedSchema().getPrimaryKey().orElse(null);
        Map<String, String> options = new HashMap<>(old.getOptions());
        if (sql instanceof SqlAlterTableOptions set) {
            options.putAll(properties(set.getPropertyList()));
        } else if (sql instanceof SqlAlterTableReset reset) {
            if (reset.getResetKeys().isEmpty() || reset.getResetKeys().contains("connector")) {
                throw invalid("ALTER TABLE RESET requires keys and cannot reset connector");
            }
            reset.getResetKeys().forEach(options::remove);
        } else if (sql instanceof SqlAlterTableSchema change) {
            change.validate();
            for (SqlNode node : change.getColumnPositions()) {
                var position = (SqlTableColumnPosition) node;
                Column column = column(position.getColumn(), types);
                int existing = indexOf(columns, column.getName());
                boolean add = change instanceof SqlAlterTableAdd;
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
            if (change.getFullConstraint().isPresent()) {
                if (change instanceof SqlAlterTableAdd && primaryKey != null) throw invalid("Primary key already exists");
                if (change instanceof SqlAlterTableModify && primaryKey == null) throw invalid("Primary key does not exist");
                primaryKey = primaryKey(change.getFullConstraint().orElseThrow());
            }
        } else if (sql instanceof SqlAlterTableDropColumn drop) {
            for (String name : identifiers(drop.getColumnList())) {
                columns.remove(requireColumn(columns, name));
            }
        } else if (sql instanceof SqlAlterTableRenameColumn rename) {
            String name = rename.getOldColumnIdentifier().getSimple();
            String replacement = rename.getNewColumnIdentifier().getSimple();
            int index = requireColumn(columns, name);
            if (indexOf(columns, replacement) >= 0) throw invalid("Column already exists: " + replacement);
            Column previousColumn = columns.get(index);
            Column renamed = previousColumn instanceof Column.MetadataColumn metadata
                    ? Column.metadata(replacement, metadata.getDataType(), metadata.getMetadataKey().orElse(null), metadata.isVirtual())
                    : Column.physical(replacement, previousColumn.getDataType());
            columns.set(index, renamed.withComment(previousColumn.getComment().orElse(null)));
            partitions.replaceAll(key -> key.equals(name) ? replacement : key);
            if (primaryKey != null) primaryKey = UniqueConstraint.primaryKey(primaryKey.getName(),
                    primaryKey.getColumns().stream().map(key -> key.equals(name) ? replacement : key).toList());
        } else if (sql instanceof SqlAlterTableDropPrimaryKey || sql instanceof SqlAlterTableDropConstraint) {
            if (primaryKey == null) throw invalid("Primary key does not exist");
            if (sql instanceof SqlAlterTableDropConstraint drop
                    && !primaryKey.getName().equals(drop.getConstraintName().getSimple())) {
                throw invalid("Constraint does not exist: " + drop.getConstraintName().getSimple());
            }
            primaryKey = null;
        } else {
            throw new UnsupportedOperationException("Unsupported table metadata alteration: " + sql.getClass().getSimpleName());
        }
        return table(columns, primaryKey, partitions, options, old.getComment(), old.getSnapshot().orElse(null));
    }

    private static Column column(SqlTableColumn sql, DataTypeFactory types) {
        Column column;
        if (sql instanceof SqlTableColumn.SqlRegularColumn physical) {
            column = Column.physical(sql.getName().getSimple(), dataType(physical.getType(), types));
        } else if (sql instanceof SqlTableColumn.SqlMetadataColumn metadata) {
            column = Column.metadata(sql.getName().getSimple(), dataType(metadata.getType(), types),
                    metadata.getMetadataAlias().orElse(null), metadata.isVirtual());
        } else throw unsupported();
        return column.withComment(sql.getComment().map(FlinkHiveDdlAdapter::literal).orElse(null));
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

    private static ResolvedCatalogTable table(List<Column> columns, UniqueConstraint primaryKey, List<String> partitions,
                                             Map<String, String> options, String comment, Long snapshot) {
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
        ResolvedSchema resolved = new ResolvedSchema(columns, List.of(), primaryKey);
        Schema schema = Schema.newBuilder().fromResolvedSchema(resolved).build();
        return new ResolvedCatalogTable(CatalogTable.of(schema, comment, partitions, options, snapshot), resolved);
    }

    static void requireSimpleSchema(SqlNodeList columns, boolean watermark) {
        if (watermark) throw unsupported();
        for (SqlNode column : columns) if (column instanceof SqlTableColumn.SqlComputedColumn) throw unsupported();
    }

    static Map<String, String> properties(SqlNodeList nodes) {
        Map<String, String> properties = new HashMap<>();
        for (SqlNode node : nodes) {
            var option = (SqlTableOption) node;
            properties.put(option.getKeyString(), option.getValueString());
        }
        return properties;
    }

    private static List<String> identifiers(SqlNodeList nodes) {
        return nodes.getList().stream().map(SqlIdentifier.class::cast).map(SqlIdentifier::getSimple).toList();
    }

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
