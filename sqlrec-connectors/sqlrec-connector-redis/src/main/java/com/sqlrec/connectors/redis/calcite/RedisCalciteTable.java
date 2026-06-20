package com.sqlrec.connectors.redis.calcite;

import com.sqlrec.common.schema.SqlRecCollection;
import com.sqlrec.common.schema.SqlRecKvTable;
import com.sqlrec.common.schema.SqlRecTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.connectors.redis.config.RedisConfig;
import com.sqlrec.connectors.redis.handler.RedisHandler;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;

import java.util.*;
import java.util.stream.Collectors;

public class RedisCalciteTable extends SqlRecKvTable {
    private RedisConfig redisConfig;
    private transient RedisHandler redisHandler;

    public RedisCalciteTable(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
        redisHandler = new RedisHandler(redisConfig);
        redisHandler.open();
        initCache(redisConfig.maxCacheSize, redisConfig.cacheTtl);
    }

    public Map<Object, List<Object[]>> getByPrimaryKeyImpl(Set<Object> keySet) {
        Set<String> keySetStr = keySet.stream()
                .map(Object::toString)
                .collect(Collectors.toSet());
        try {
            Map<String, List<Object[]>> scanData = redisHandler.scan(keySetStr).get();
            Map<Object, List<Object[]>> ret = new HashMap<>();
            for (Object key : keySet) {
                ret.put(key, scanData.getOrDefault(key.toString(), new ArrayList<>()));
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DataTypeUtils.getRelDataType(typeFactory, redisConfig.fieldSchemas);
    }

    @Override
    public Collection getModifiableCollection() {
        return new RedisCollection(this, redisHandler);
    }

    @Override
    protected Enumerable<Object[]> scanImpl(List<RexNode> filters) {
        throw new UnsupportedOperationException("scan is not support by redis");
    }

    @Override
    public int getPrimaryKeyIndex() {
        return redisConfig.primaryKeyIndex;
    }

    public static class RedisCollection extends SqlRecCollection {
        private final RedisCalciteTable table;
        private final RedisHandler redisHandler;

        public RedisCollection(RedisCalciteTable table, RedisHandler redisHandler) {
            super(table.getTableName());
            this.table = table;
            this.redisHandler = redisHandler;
        }

        @Override
        public SqlRecTable getSqlRecTable() {
            return table;
        }

        @Override
        protected boolean addImpl(Object[] objects) {
            redisHandler.insert(objects);
            return true;
        }

        @Override
        protected boolean removeImpl(Object[] objects) {
            redisHandler.delete(objects);
            return true;
        }

        @Override
        protected boolean addAllImpl(Collection<? extends Object[]> c) {
            redisHandler.batchInsert(c);
            return true;
        }

        @Override
        protected boolean removeAllImpl(Collection<?> c) {
            redisHandler.batchDelete((Collection<? extends Object[]>) c);
            return true;
        }
    }
}