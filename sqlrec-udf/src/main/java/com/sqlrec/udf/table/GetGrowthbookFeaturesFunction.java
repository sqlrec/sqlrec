package com.sqlrec.udf.table;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.sqlrec.common.runtime.ExecuteContext;
import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.RowJsonEncoder;
import growthbook.sdk.java.GrowthBook;
import growthbook.sdk.java.callback.FeatureRefreshCallback;
import growthbook.sdk.java.model.GBContext;
import growthbook.sdk.java.model.ExperimentResult;
import growthbook.sdk.java.model.FeatureResult;
import growthbook.sdk.java.repository.GBFeaturesRepository;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GetGrowthbookFeaturesFunction {

    static final Cache<String, FeatureClient> CLIENT_CACHE = Caffeine.newBuilder()
            .maximumSize(64)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .scheduler(Scheduler.systemScheduler())
            .removalListener((String key, FeatureClient client, RemovalCause cause) -> {
                if (client != null) {
                    client.close();
                }
            })
            .build();

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

        List<Object[]> trackingData = new ArrayList<>();
        List<RelDataTypeField> userFields = usertable.getDataFields();
        List<Object[]> userEnumerable = usertable.scan(null).toList();

        if (userEnumerable.size() != 1) {
            throw new RuntimeException("usertable should contain 1 row");
        }

        Object[] row = userEnumerable.get(0);
        String attributesJson = RowJsonEncoder.toJsonByFields(row, userFields);
        GrowthBook gb = CLIENT_CACHE.get(cacheKey(apiHost, clientKey),
                key -> new FeatureClient(apiHost, clientKey)).forUser(attributesJson);

        for (String featureKey : featureKeys) {
            FeatureResult<?> result = gb.evalFeature(featureKey, Object.class);

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

    // SDK 0.10.10's multi-user facade has a process-wide static repository.
    // Own the repository here and keep mutable evaluation state request-local.
    static final class FeatureClient {
        private final GBFeaturesRepository repository;
        private volatile GBContext snapshot;

        FeatureClient(String apiHost, String clientKey) {
            repository = GBFeaturesRepository.builder()
                    .apiHost(apiHost)
                    .clientKey(clientKey)
                    // The SDK's default disk cache is shared across keys.
                    .isCacheDisabled(true)
                    .build();
            repository.onFeaturesRefresh(new FeatureRefreshCallback() {
                @Override
                public void onRefresh(String featuresJson) {
                    snapshot = GBContext.builder()
                            .features(repository.getParsedFeatures())
                            .savedGroups(repository.getParsedSavedGroups())
                            .build();
                }

                @Override
                public void onError(Throwable error) {
                    // The repository logs refresh failures. Retain the last
                    // successful snapshot for existing clients.
                }
            });
            try {
                repository.initialize();
                if (snapshot == null) {
                    throw new IllegalStateException("No GrowthBook feature payload received");
                }
            } catch (Exception error) {
                close();
                throw new RuntimeException("Failed to initialize GrowthBook client, "
                        + "please check apiHost and clientKey", error);
            }
        }

        GrowthBook forUser(String attributesJson) {
            GBContext current = snapshot;
            return new GrowthBook(GBContext.builder()
                    .features(current.getFeatures())
                    .savedGroups(current.getSavedGroups())
                    .attributesJson(attributesJson)
                    .build());
        }

        void close() {
            repository.shutdown();
        }
    }
}
