package com.sqlrec.connectors.milvus.calcite;

import com.sqlrec.common.schema.HmsTableFactory;
import com.sqlrec.common.utils.HiveTableUtils;
import com.sqlrec.connectors.milvus.config.MilvusConfig;
import com.sqlrec.connectors.milvus.config.MilvusOptions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MilvusCalciteTableFactory implements HmsTableFactory {
    @Override
    public Table getTableFromHmsTable(org.apache.hadoop.hive.metastore.api.Table tableObj) {
        Map<String, String> flinkTableOptions = HiveTableUtils.getFlinkTableOptions(tableObj);
        MilvusConfig milvusConfig = MilvusOptions.getMilvusConfig(flinkTableOptions);
        milvusConfig.fieldSchemas = HiveTableUtils.parse(tableObj);
        milvusConfig.primaryKey = HiveTableUtils.getTablePrimaryKey(tableObj);
        milvusConfig.primaryKeyIndex = HiveTableUtils.getTablePrimaryKeyIndex(
                milvusConfig.fieldSchemas, milvusConfig.primaryKey
        );

        return new MilvusCalciteTable(milvusConfig);
    }

    @Override
    public String getConnectorName() {
        return MilvusOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public List<RelOptRule> getRules() {
        return new ArrayList<>();
    }
}
