package com.sqlrec.udf.table;

import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.JsonUtils;
import growthbook.sdk.java.model.ExperimentResult;
import growthbook.sdk.java.model.FeatureResult;
import growthbook.sdk.java.multiusermode.GrowthBookClient;
import growthbook.sdk.java.multiusermode.configurations.Options;
import growthbook.sdk.java.multiusermode.configurations.UserContext;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class GetGrowthbookFeaturesFunction {

    private static final ConcurrentHashMap<String, GrowthBookClient> CLIENT_CACHE = new ConcurrentHashMap<>();

    private static String cacheKey(String apiHost, String clientKey) {
        return apiHost + "|" + clientKey;
    }

    public CacheTable evaluate(ExecuteContext context, String apiHost, String clientKey,
                               CacheTable usertable, String... featureKeys) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        if (apiHost == null || apiHost.isEmpty()) {
            throw new IllegalArgumentException("apiHost cannot be null or empty");
        }
        if (clientKey == null || clientKey.isEmpty()) {
            throw new IllegalArgumentException("clientKey cannot be null or empty");
        }
        if (usertable == null) {
            throw new IllegalArgumentException("usertable cannot be null");
        }
        if (featureKeys == null || featureKeys.length == 0) {
            throw new IllegalArgumentException("featureKeys cannot be null or empty");
        }

        // Get or create shared GrowthBookClient
        GrowthBookClient gb = getOrCreateClient(apiHost, clientKey);

        List<Object[]> trackingData = new ArrayList<>();
        List<RelDataTypeField> userFields = usertable.getDataFields();
        List<Object[]> userEnumerable = usertable.scan(null).toList();

        if (userEnumerable.size() != 1) {
            throw new RuntimeException("usertable should contain 1 row");
        }

        Object[] row = userEnumerable.get(0);
        String attributesJson = JsonUtils.toJsonByFields(row, userFields);
        UserContext userContext = UserContext.builder()
                .attributesJson(attributesJson)
                .build();

        for (String featureKey : featureKeys) {
            FeatureResult<?> result = gb.evalFeature(featureKey, Object.class, userContext);

            if (result != null && result.getExperimentResult() != null) {
                ExperimentResult<?> expResult = result.getExperimentResult();

                String experimentId = result.getExperiment() != null
                        ? result.getExperiment().getKey() : null;
                String variationId = expResult.getVariationId() != null
                        ? expResult.getVariationId().toString() : null;
                String userId = expResult.getHashValue();

                // Set AB experiment parameters as variables via ExecuteContext
                context.setVariable(featureKey, expResult.getValue() != null ? expResult.getValue().toString() : null);

                // Collect tracking data for metrics calculation
                trackingData.add(new Object[]{experimentId, variationId, userId});
            }
        }

        // Build output CacheTable with tracking callback data for metrics calculation
        List<RelDataTypeField> dataFields = new ArrayList<>();
        dataFields.add(DataTypeUtils.getRelDataTypeField("experiment_id", 0, SqlTypeName.VARCHAR));
        dataFields.add(DataTypeUtils.getRelDataTypeField("variation_id", 1, SqlTypeName.VARCHAR));
        dataFields.add(DataTypeUtils.getRelDataTypeField("user_id", 2, SqlTypeName.VARCHAR));

        return new CacheTable("growthbook_tracking", Linq4j.asEnumerable(trackingData), dataFields);
    }

    private static GrowthBookClient getOrCreateClient(String apiHost, String clientKey) {
        String key = cacheKey(apiHost, clientKey);
        return CLIENT_CACHE.computeIfAbsent(key, k -> {
            Options options = Options.builder()
                    .apiHost(apiHost)
                    .clientKey(clientKey)
                    .build();
            GrowthBookClient client = new GrowthBookClient(options);
            boolean initialized = client.initialize();
            if (!initialized) {
                throw new RuntimeException("Failed to initialize GrowthBook client, " +
                        "please check apiHost and clientKey");
            }
            return client;
        });
    }
}
