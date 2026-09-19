package com.sqlrec.udf.table;

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
                {"item1", 0.1, Arrays.asList(1.0, 0.0)},
                {"item2", 0.01, Arrays.asList(-1.0, 0.0)},
                {"item3", 1000.0, Arrays.asList(0.0, 1.0)},
        };
        CacheTable input = createTable(data);

        CacheTable output = function.evaluate(input, "embedding", "score", "0.0", "2");
        List<Object[]> result = collectRows(output);

        assertEquals(2, result.size());
        // Scores differ by five orders of magnitude, but theta=0 must ignore them.
        // item1 and item2 point in opposite directions and map to zero similarity.
        List<String> names = result.stream().map(r -> (String) r[0]).collect(Collectors.toList());
        assertTrue(names.contains("item1") && names.contains("item2"),
                "With theta=0, score must not override the most diverse pair");
    }

    /**
     * Test that higher theta (closer to 1) gives more weight to relevance.
     * With a high theta, the highest-scoring item should be preferred even if
     * its embedding is similar to already-selected items.
     */
    @Test
    public void testThetaChangesRelevanceDiversityTradeoff() {
        Object[][] data = {
                {"highest", 10.0, Arrays.asList(1.0, 0.0)},
                {"relevant", 9.0, Arrays.asList(0.999, 0.04)},
                {"diverse", 1.0, Arrays.asList(-1.0, 0.0)},
        };
        CacheTable input = createTable(data);

        List<String> lowTheta = selectedNames(
                function.evaluate(input, "embedding", "score", "0.1", "2"));
        List<String> highTheta = selectedNames(
                function.evaluate(input, "embedding", "score", "0.9", "2"));

        assertEquals(Arrays.asList("highest", "diverse"), lowTheta,
                "Low theta should sacrifice relevance for diversity");
        assertEquals(Arrays.asList("highest", "relevant"), highTheta,
                "High theta should favor relevance despite embedding similarity");
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

    @Test
    public void testLargeFiniteScoresDoNotOverflowKernel() {
        Object[][] normalScores = {
                {"item1", 3.0, new double[]{1.0, 0.0, 0.0}},
                {"item2", 2.0, new double[]{0.9, 0.1, 0.0}},
                {"item3", 1.0, new double[]{0.0, 1.0, 0.0}},
                {"item4", 0.5, new double[]{0.0, 0.0, 1.0}},
        };
        Object[][] shiftedScores = {
                {"item1", 1003.0, new double[]{1.0, 0.0, 0.0}},
                {"item2", 1002.0, new double[]{0.9, 0.1, 0.0}},
                {"item3", 1001.0, new double[]{0.0, 1.0, 0.0}},
                {"item4", 1000.5, new double[]{0.0, 0.0, 1.0}},
        };

        List<String> normal = selectedNames(function.evaluate(
                createTable(normalScores), "embedding", "score", "0.5", "4"));
        List<String> shifted = selectedNames(function.evaluate(
                createTable(shiftedScores), "embedding", "score", "0.5", "4"));

        assertEquals(normal, shifted,
                "Adding a common score offset must not change fixed-cardinality DPP ranking");
    }

    @Test
    public void testDoubleArrayEmbeddingIsNotModified() {
        double[] embedding = {3.0, 4.0};
        CacheTable input = createTable(new Object[][]{
                {"item1", 1.0, embedding},
        });

        function.evaluate(input, "embedding", "score", "0.5", "1");

        assertArrayEquals(new double[]{3.0, 4.0}, embedding);
    }

    @Test
    public void testFastGreedyMatchesNaiveDeterminantGreedyBeyondSecondSelection() {
        java.util.Random random = new java.util.Random(20260919L);

        for (int round = 0; round < 25; round++) {
            Object[][] data = randomData(random, 7, 8);
            List<String> expected = naiveGreedySelection(data, 0.45, 5);
            List<String> actual = selectedNames(function.evaluate(
                    createTable(data), "embedding", "score", "0.45", "5"));

            assertEquals(expected, actual, "Greedy order differs in random round " + round);
        }
    }

    @Test
    public void testEmbeddingMagnitudeDoesNotChangeSelection() {
        Object[][] normalizedScale = {
                {"item1", 2.0, Arrays.asList(1.0, 0.0, 0.0)},
                {"item2", 1.8, Arrays.asList(0.8, 0.2, 0.0)},
                {"item3", 1.5, Arrays.asList(0.0, 1.0, 0.0)},
                {"item4", 1.0, Arrays.asList(0.0, 0.0, 1.0)},
        };
        Object[][] arbitraryScale = {
                {"item1", 2.0, Arrays.asList(100.0, 0.0, 0.0)},
                {"item2", 1.8, Arrays.asList(4.0, 1.0, 0.0)},
                {"item3", 1.5, Arrays.asList(0.0, 0.01, 0.0)},
                {"item4", 1.0, Arrays.asList(0.0, 0.0, 7.0)},
        };

        List<String> expected = selectedNames(function.evaluate(
                createTable(normalizedScale), "embedding", "score", "0.4", "3"));
        List<String> actual = selectedNames(function.evaluate(
                createTable(arbitraryScale), "embedding", "score", "0.4", "3"));

        assertEquals(expected, actual);
    }

    @Test
    public void testRejectsNonFiniteThetaScoreAndEmbedding() {
        CacheTable valid = createTable(new Object[][]{
                {"item1", 1.0, Arrays.asList(1.0, 0.0)},
        });
        CacheTable infiniteScore = createTable(new Object[][]{
                {"item1", Double.POSITIVE_INFINITY, Arrays.asList(1.0, 0.0)},
        });
        CacheTable nanEmbedding = createTable(new Object[][]{
                {"item1", 1.0, Arrays.asList(Double.NaN, 0.0)},
        });

        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> function.evaluate(valid, "embedding", "score", "NaN", "1")),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> function.evaluate(valid, "embedding", "score", "Infinity", "1")),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> function.evaluate(infiniteScore, "embedding", "score", "0.5", "1")),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> function.evaluate(nanEmbedding, "embedding", "score", "0.5", "1"))
        );
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

    private Object[][] randomData(java.util.Random random, int itemCount, int dimension) {
        Object[][] data = new Object[itemCount][];
        for (int i = 0; i < itemCount; i++) {
            List<Double> embedding = new ArrayList<>(dimension);
            for (int j = 0; j < dimension; j++) {
                embedding.add(random.nextGaussian());
            }
            data[i] = new Object[]{"item" + i, 0.2 + 2.0 * random.nextDouble(), embedding};
        }
        return data;
    }

    /**
     * Independent small-input oracle: construct the DPP kernel from the public contract and
     * choose each item by directly recomputing the candidate submatrix determinant.
     */
    private List<String> naiveGreedySelection(Object[][] data, double theta, int maxLength) {
        int itemCount = data.length;
        double alpha = theta / (2.0 * (1.0 - theta));
        double[][] embeddings = new double[itemCount][];
        double[] quality = new double[itemCount];

        for (int i = 0; i < itemCount; i++) {
            List<?> values = (List<?>) data[i][2];
            double normSquared = 0.0;
            embeddings[i] = new double[values.size()];
            for (int j = 0; j < values.size(); j++) {
                double value = ((Number) values.get(j)).doubleValue();
                embeddings[i][j] = value;
                normSquared += value * value;
            }
            double norm = Math.sqrt(normSquared);
            for (int j = 0; j < embeddings[i].length; j++) {
                embeddings[i][j] /= norm;
            }
            quality[i] = Math.exp(alpha * ((Number) data[i][1]).doubleValue());
        }

        double[][] kernel = new double[itemCount][itemCount];
        for (int i = 0; i < itemCount; i++) {
            for (int j = 0; j < itemCount; j++) {
                double cosine = 0.0;
                for (int d = 0; d < embeddings[i].length; d++) {
                    cosine += embeddings[i][d] * embeddings[j][d];
                }
                kernel[i][j] = quality[i] * (1.0 + cosine) / 2.0 * quality[j];
            }
        }

        List<Integer> selected = new ArrayList<>();
        while (selected.size() < Math.min(maxLength, itemCount)) {
            int bestItem = -1;
            double bestDeterminant = Double.NEGATIVE_INFINITY;
            for (int candidate = 0; candidate < itemCount; candidate++) {
                if (selected.contains(candidate)) {
                    continue;
                }
                List<Integer> indices = new ArrayList<>(selected);
                indices.add(candidate);
                double candidateDeterminant = determinant(kernel, indices);
                if (candidateDeterminant > bestDeterminant) {
                    bestDeterminant = candidateDeterminant;
                    bestItem = candidate;
                }
            }
            selected.add(bestItem);
        }

        return selected.stream()
                .map(index -> (String) data[index][0])
                .collect(Collectors.toList());
    }

    private double determinant(double[][] source, List<Integer> indices) {
        int size = indices.size();
        double[][] matrix = new double[size][size];
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                matrix[i][j] = source[indices.get(i)][indices.get(j)];
            }
        }

        double determinant = 1.0;
        for (int column = 0; column < size; column++) {
            int pivot = column;
            for (int row = column + 1; row < size; row++) {
                if (Math.abs(matrix[row][column]) > Math.abs(matrix[pivot][column])) {
                    pivot = row;
                }
            }
            if (Math.abs(matrix[pivot][column]) < 1e-14) {
                return 0.0;
            }
            if (pivot != column) {
                double[] swap = matrix[pivot];
                matrix[pivot] = matrix[column];
                matrix[column] = swap;
                determinant = -determinant;
            }

            double pivotValue = matrix[column][column];
            determinant *= pivotValue;
            for (int row = column + 1; row < size; row++) {
                double factor = matrix[row][column] / pivotValue;
                for (int remaining = column + 1; remaining < size; remaining++) {
                    matrix[row][remaining] -= factor * matrix[column][remaining];
                }
            }
        }
        return determinant;
    }

    private List<String> selectedNames(CacheTable table) {
        return collectRows(table).stream()
                .map(row -> (String) row[0])
                .collect(Collectors.toList());
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
