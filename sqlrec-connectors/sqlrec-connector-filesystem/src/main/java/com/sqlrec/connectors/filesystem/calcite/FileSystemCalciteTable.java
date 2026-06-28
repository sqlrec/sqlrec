package com.sqlrec.connectors.filesystem.calcite;

import com.sqlrec.common.schema.SqlRecCollection;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.filesystem.config.FileSystemConfig;
import com.sqlrec.connectors.filesystem.handler.FileSystemHandler;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileSystemCalciteTable extends SqlRecKvTable {
    private final FileSystemConfig fileSystemConfig;
    private transient FileSystemHandler fileSystemHandler;

    public FileSystemCalciteTable(FileSystemConfig fileSystemConfig) {
        this.fileSystemConfig = fileSystemConfig;
        this.fileSystemHandler = new FileSystemHandler(fileSystemConfig);
    }

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        if (filters == null || filters.isEmpty()) {
            List<Object[]> rows = fileSystemHandler.scan();
            return Linq4j.asEnumerable(rows);
        }
        throw new UnsupportedOperationException("scan by filter is not support by filesystem");
    }

    @Override
    public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
        return fileSystemHandler.getByPrimaryKey(keySet);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, fileSystemConfig.fieldSchemas);
    }

    @Override
    public @Nullable Collection getModifiableCollection() {
        return new FileSystemCollection(this, fileSystemHandler);
    }

    @Override
    public int getPrimaryKeyIndex() {
        return fileSystemConfig.primaryKeyIndex;
    }

    public static class FileSystemCollection extends SqlRecCollection {
        private final FileSystemCalciteTable table;
        private final FileSystemHandler fileSystemHandler;

        public FileSystemCollection(FileSystemCalciteTable table, FileSystemHandler fileSystemHandler) {
            super(table.getTableName());
            this.table = table;
            this.fileSystemHandler = fileSystemHandler;
        }

        @Override
        public SqlRecTable getSqlRecTable() {
            return table;
        }

        @Override
        protected boolean addImpl(Object[] objects) {
            return fileSystemHandler.upsert(objects);
        }

        @Override
        protected boolean removeImpl(Object[] objects) {
            return fileSystemHandler.delete(objects);
        }
    }
}
