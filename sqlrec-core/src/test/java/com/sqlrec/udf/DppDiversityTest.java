package com.sqlrec.udf;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.udf.table.DppDiversity;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class DppDiversityTest {
    private DppDiversity function;

    @BeforeEach
    public void setUp() {
        function = new DppDiversity();
    }

    /**
     * Test DPP with identical embeddings: all items should still be returned
     * (no early termination), but the first selected item should have the highest score.
     */
    @Test
    public void testIdenticalEmbeddingsReturnsAll() {
        // 6 items, all with the same embedding, different scores
        List<Double> sameEmb = Arrays.asList(1.0, 0.0, 0.0);
        Object[][] data = {
                {"item1", 0.9, sameEmb},
                {"item2", 0.8, sameEmb},
                {"item3", 0.7, sameEmb},
                {"item4", 0.6, sameEmb},
                {"item5", 0.5, sameEmb},
                {"item6", 0.4, sameEmb},
        };
        CacheTable input = createTable(data);

        // theta=0.5 (balanced), maxLength=6
        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "6");
        List<Object[]> result = collectRows(output);

        // All items should be returned (no early termination)
        assertEquals(6, result.size(), "Should return all items even with identical embeddings");
        // The first selected item should be the one with highest score
        assertEquals("item1", result.get(0)[0]);
    }

    /**
     * Test DPP diversity with orthogonal embeddings: items with completely different
     * embeddings should all be selected since they are maximally diverse.
     */
    @Test
    public void testOrthogonalEmbeddingsAllSelected() {
        // 3 items with orthogonal embeddings (standard basis vectors)
        Object[][] data = {
                {"item1", 1.0, Arrays.asList(1.0, 0.0, 0.0)},
                {"item2", 1.0, Arrays.asList(0.0, 1.0, 0.0)},
                {"item3", 1.0, Arrays.asList(0.0, 0.0, 1.0)},
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "3");
        List<Object[]> result = collectRows(output);

        // All 3 orthogonal items should be selected
        assertEquals(3, result.size(), "Orthogonal embeddings should all be selected");
    }

    /**
     * Test that DPP selects items from different clusters when embeddings form clusters.
     * Two clusters of items: cluster A (items 1-3) and cluster B (items 4-6).
     * DPP should pick items from both clusters.
     */
    @Test
    public void testClusteredEmbeddingsSelectFromBothClusters() {
        // Cluster A: items with embedding close to [1, 0]
        // Cluster B: items with embedding close to [0, 1]
        Object[][] data = {
                {"A1", 1.0, Arrays.asList(1.0, 0.0)},
                {"A2", 1.0, Arrays.asList(0.99, 0.01)},
                {"A3", 1.0, Arrays.asList(0.98, 0.02)},
                {"B1", 1.0, Arrays.asList(0.0, 1.0)},
                {"B2", 1.0, Arrays.asList(0.01, 0.99)},
                {"B3", 1.0, Arrays.asList(0.02, 0.98)},
        };
        CacheTable input = createTable(data);

        // Select 2 items with balanced theta
        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "2");
        List<Object[]> result = collectRows(output);

        assertEquals(2, result.size());

        // One item from each cluster
        List<String> names = result.stream().map(r -> (String) r[0]).collect(Collectors.toList());
        boolean hasA = names.stream().anyMatch(n -> n.startsWith("A"));
        boolean hasB = names.stream().anyMatch(n -> n.startsWith("B"));
        assertTrue(hasA && hasB, "Should select one item from each cluster");
    }

    /**
     * Test theta=0 (pure diversity): scores are ignored, selection is purely based on diversity.
     * All items have the same score, so only embedding diversity matters.
     */
    @Test
    public void testThetaZeroPureDiversity() {
        Object[][] data = {
                {"item1", 1.0, Arrays.asList(1.0, 0.0)},
                {"item2", 1.0, Arrays.asList(0.0, 1.0)},
                {"item3", 1.0, Arrays.asList(0.707, 0.707)},
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.0", "2");
        List<Object[]> result = collectRows(output);

        assertEquals(2, result.size());
        // With theta=0, all scores are exp(0)=1, so selection is purely diversity-driven
        // The two most dissimilar items (item1 and item2, orthogonal) should be selected
        List<String> names = result.stream().map(r -> (String) r[0]).collect(Collectors.toList());
        assertTrue(names.contains("item1") && names.contains("item2"),
                "With theta=0, the two most dissimilar (orthogonal) items should be selected");
    }

    /**
     * Test that higher theta (closer to 1) gives more weight to relevance.
     * With a high theta, the highest-scoring item should be preferred even if
     * its embedding is similar to already-selected items.
     */
    @Test
    public void testHighThetaFavorsRelevance() {
        // Two distinct embeddings, but one has much higher score
        Object[][] data = {
                {"highScore", 10.0, Arrays.asList(1.0, 0.0)},
                {"lowScore1", 0.1, Arrays.asList(0.0, 1.0)},
                {"lowScore2", 0.1, Arrays.asList(-1.0, 0.0)},  // similar direction to highScore
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.9", "2");
        List<Object[]> result = collectRows(output);

        assertEquals(2, result.size());
        // With high theta, highScore must be selected first
        assertEquals("highScore", result.get(0)[0]);
    }

    /**
     * Test maxLength limits the output size.
     */
    @Test
    public void testMaxLengthLimit() {
        Object[][] data = {
                {"item1", 1.0, Arrays.asList(1.0, 0.0, 0.0)},
                {"item2", 1.0, Arrays.asList(0.0, 1.0, 0.0)},
                {"item3", 1.0, Arrays.asList(0.0, 0.0, 1.0)},
                {"item4", 1.0, Arrays.asList(0.5, 0.5, 0.0)},
                {"item5", 1.0, Arrays.asList(0.5, 0.0, 0.5)},
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "3");
        List<Object[]> result = collectRows(output);

        assertEquals(3, result.size(), "Output size should be limited by maxLength");
    }

    /**
     * Test that rows with null embedding are filtered out.
     */
    @Test
    public void testNullEmbeddingFiltered() {
        Object[][] data = {
                {"item1", 1.0, Arrays.asList(1.0, 0.0)},
                {"item2", 0.8, null},
                {"item3", 0.6, Arrays.asList(0.0, 1.0)},
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "10");
        List<Object[]> result = collectRows(output);

        assertEquals(2, result.size(), "Rows with null embedding should be filtered out");
        List<String> names = result.stream().map(r -> (String) r[0]).collect(Collectors.toList());
        assertTrue(names.contains("item1"));
        assertTrue(names.contains("item3"));
        assertFalse(names.contains("item2"));
    }

    /**
     * Test that rows with null score are filtered out.
     */
    @Test
    public void testNullScoreFiltered() {
        Object[][] data = {
                {"item1", 1.0, Arrays.asList(1.0, 0.0)},
                {"item2", null, Arrays.asList(0.0, 1.0)},
                {"item3", 0.6, Arrays.asList(0.5, 0.5)},
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "10");
        List<Object[]> result = collectRows(output);

        assertEquals(2, result.size(), "Rows with null score should be filtered out");
    }

    /**
     * Test empty input table returns empty output.
     */
    @Test
    public void testEmptyInput() {
        CacheTable input = createTable(new Object[][]{});

        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "10");
        List<Object[]> result = collectRows(output);

        assertEquals(0, result.size());
    }

    /**
     * Test invalid theta values.
     */
    @Test
    public void testInvalidThetaNegative() {
        CacheTable input = createTable(new Object[][]{{"item1", 1.0, Arrays.asList(1.0, 0.0)}});
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "embedding", "score", "-0.1", "1");
        });
    }

    @Test
    public void testInvalidThetaOne() {
        CacheTable input = createTable(new Object[][]{{"item1", 1.0, Arrays.asList(1.0, 0.0)}});
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "embedding", "score", "1.0", "1");
        });
    }

    /**
     * Test invalid maxLength.
     */
    @Test
    public void testInvalidMaxLengthZero() {
        CacheTable input = createTable(new Object[][]{{"item1", 1.0, Arrays.asList(1.0, 0.0)}});
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "embedding", "score", "0.5", "0");
        });
    }

    @Test
    public void testInvalidMaxLengthNegative() {
        CacheTable input = createTable(new Object[][]{{"item1", 1.0, Arrays.asList(1.0, 0.0)}});
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "embedding", "score", "0.5", "-1");
        });
    }

    /**
     * Test column name not found.
     */
    @Test
    public void testEmbeddingColumnNotFound() {
        CacheTable input = createTable(new Object[][]{{"item1", 1.0, Arrays.asList(1.0, 0.0)}});
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "nonexistent", "score", "0.5", "1");
        });
    }

    @Test
    public void testScoreColumnNotFound() {
        CacheTable input = createTable(new Object[][]{{"item1", 1.0, Arrays.asList(1.0, 0.0)}});
        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "embedding", "nonexistent", "0.5", "1");
        });
    }

    /**
     * Test embedding dimension mismatch throws exception.
     */
    @Test
    public void testEmbeddingDimensionMismatch() {
        Object[][] data = {
                {"item1", 1.0, Arrays.asList(1.0, 0.0)},
                {"item2", 0.8, Arrays.asList(0.0, 1.0, 0.0)},  // different dimension
        };
        CacheTable input = createTable(data);

        assertThrows(IllegalArgumentException.class, () -> {
            function.evaluate(input, "embedding", "score", "0.5", "2");
        });
    }

    /**
     * Test negative scores are handled (clipped to small positive value).
     */
    @Test
    public void testNegativeScoresHandled() {
        Object[][] data = {
                {"item1", -0.5, Arrays.asList(1.0, 0.0)},
                {"item2", 1.0, Arrays.asList(0.0, 1.0)},
        };
        CacheTable input = createTable(data);

        // Should not throw, negative score is clipped
        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "2");
        List<Object[]> result = collectRows(output);

        assertEquals(2, result.size());
    }

    /**
     * Test that DPP produces more diverse results than pure score-based ranking.
     * Given items where top-scoring items are all similar, DPP should still
     * produce a more diverse selection.
     */
    @Test
    public void testDppMoreDiverseThanScoreRanking() {
        // 4 items: 3 similar high-score, 1 different lower-score
        Object[][] data = {
                {"similar1", 1.0, Arrays.asList(1.0, 0.0)},
                {"similar2", 0.95, Arrays.asList(0.99, 0.01)},
                {"similar3", 0.9, Arrays.asList(0.98, 0.02)},
                {"different", 0.8, Arrays.asList(0.0, 1.0)},
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "2");
        List<Object[]> result = collectRows(output);

        assertEquals(2, result.size());
        // DPP with theta=0.5 should select "different" as one of the 2 items
        // because it adds diversity, even though it has a lower score
        List<String> names = result.stream().map(r -> (String) r[0]).collect(Collectors.toList());
        assertTrue(names.contains("different"),
                "DPP should select the diverse item even with lower score");
    }

    /**
     * Test with single item.
     */
    @Test
    public void testSingleItem() {
        Object[][] data = {
                {"item1", 1.0, Arrays.asList(1.0, 0.0)},
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "5");
        List<Object[]> result = collectRows(output);

        assertEquals(1, result.size());
        assertEquals("item1", result.get(0)[0]);
    }

    /**
     * Test that maxLength larger than item count returns all items (up to available).
     */
    @Test
    public void testMaxLengthExceedsItemCount() {
        Object[][] data = {
                {"item1", 1.0, Arrays.asList(1.0, 0.0)},
                {"item2", 0.8, Arrays.asList(0.0, 1.0)},
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", "100");
        List<Object[]> result = collectRows(output);

        assertEquals(2, result.size(), "Should return all items when maxLength exceeds item count");
    }

    /**
     * Performance test: 1000 items, 256-dim random embeddings, select top 300.
     */
    @Test
    public void testPerformance1000Items256Dim() {
        int itemSize = 300;
        int dim = 1024;
        int maxLen = 100;
        java.util.Random rand = new java.util.Random(42);

        Object[][] data = new Object[itemSize][];
        for (int i = 0; i < itemSize; i++) {
            List<Double> emb = new ArrayList<>(dim);
            for (int j = 0; j < dim; j++) {
                emb.add(rand.nextGaussian());
            }
            data[i] = new Object[]{"item" + i, rand.nextDouble(), emb};
        }
        CacheTable input = createTable(data);

        long start = System.currentTimeMillis();
        CacheTable output = function.evaluate(input, "embedding", "score", "0.5", String.valueOf(maxLen));
        long elapsed = System.currentTimeMillis() - start;

        List<Object[]> result = collectRows(output);
        assertEquals(maxLen, result.size());

        System.out.println("=== DPP Performance Test ===");
        System.out.println("Items: " + itemSize + ", Dim: " + dim + ", MaxLength: " + maxLen);
        System.out.println("Execution time: " + elapsed + "ms");
    }

    private CacheTable createTable(Object[][] data) {
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : data) {
            rows.add(row);
        }
        return new CacheTable("test", Linq4j.asEnumerable(rows), createFields());
    }

    private List<RelDataTypeField> createFields() {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add(DataTypeUtils.getRelDataTypeField("name", 0, SqlTypeName.VARCHAR));
        fields.add(DataTypeUtils.getRelDataTypeField("score", 1, SqlTypeName.DOUBLE));
        fields.add(DataTypeUtils.getRelDataTypeField("embedding", 2, SqlTypeName.ARRAY));
        return fields;
    }

    private List<Object[]> collectRows(CacheTable table) {
        List<Object[]> result = new ArrayList<>();
        table.scan(null).forEach(result::add);
        return result;
    }
}
