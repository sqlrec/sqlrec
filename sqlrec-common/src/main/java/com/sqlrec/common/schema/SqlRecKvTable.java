package com.sqlrec.common.schema;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;

import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class SqlRecKvTable extends SqlRecTable implements ModifiableTable, FilterableTable {
    private Cache<Object, List<Object[]>> cache;

    public int getPrimaryKeyIndex() {
        throw new UnsupportedOperationException("getPrimaryKeyIndex not support");
    }

    public Map<Object, List<Object[]>> getByPrimaryKey(Set<Object> keySet) {
        throw new UnsupportedOperationException("getByPrimaryKey not support");
    }

    public void initCache(int maxSize, long expireAfterWrite) {
        if (maxSize <= 0 || expireAfterWrite <= 0) {
            return;
        }

        cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireAfterWrite, TimeUnit.SECONDS)
                .build();
    }

    public Map<Object, List<Object[]>> getByPrimaryKeyWithCache(Set<Object> keySet) {
        if (cache == null) {
            return getByPrimaryKey(keySet);
        }

        Map<Object, List<Object[]>> result = new HashMap<>(keySet.size());
        Set<Object> missKeys = new HashSet<>();
        for (Object key : keySet) {
            List<Object[]> list = cache.getIfPresent(key);
            if (list != null) {
                result.put(key, list);
            } else {
                missKeys.add(key);
            }
        }
        if (!missKeys.isEmpty()) {
            Map<Object, List<Object[]>> missKeyResult = getByPrimaryKey(missKeys);
            result.putAll(missKeyResult);
            for (Object key : missKeyResult.keySet()) {
                cache.put(key, missKeyResult.get(key));
            }
        }
        return result;
    }

    public boolean onlyFilterByPrimaryKey() {
        return true;
    }
}
