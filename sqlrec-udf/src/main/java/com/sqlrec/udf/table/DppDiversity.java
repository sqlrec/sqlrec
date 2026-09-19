package com.sqlrec.udf.table;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.common.utils.DataTransformUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.ejml.data.DMatrixRMaj;

import java.util.ArrayList;
import java.util.List;

/**
 * DPP (Determinantal Point Process) diversity UDF.
 * <p>
 * Implements the fast greedy MAP inference algorithm from:
 * "Fast Greedy MAP Inference for Determinantal Point Process to Improve Recommendation Diversity" (Hulu, NIPS 2018)
 * <p>
 * Steps:
 * 1. Extract relevance scores and embedding vectors from the input table.
 * 2. Clip negative scores to a small positive value, then apply exponential transform:
 *    score = exp(alpha * r), where alpha = theta / (2 * (1 - theta)), theta in [0, 1).
 * 3. L2 normalize embedding vectors.
 * 4. Build kernel matrix L = Diag(scores) * S * Diag(scores), where S is the similarity matrix.
 * 5. Run greedy DPP MAP inference to select diverse items.
 */
public final class DppDiversity {
    private static final double MAX_LOG_KERNEL_FACTOR =
            0.5 * Math.log(Double.MAX_VALUE) - Math.log(2.0);

    public CacheTable evaluate(
            CacheTable input,
            String embeddingColumnName,
            String scoreColumnName,
            String theta,
            String maxLength
    ) {
        double thetaVal = Double.parseDouble(theta);
        int maxLengthVal = Integer.parseInt(maxLength);
        if (!Double.isFinite(thetaVal) || thetaVal < 0 || thetaVal >= 1) {
            throw new IllegalArgumentException("theta must be in [0, 1), got: " + theta);
        }
        if (maxLengthVal <= 0) {
            throw new IllegalArgumentException("maxLength must be positive, got: " + maxLength);
        }

        // Find column indices
        int embeddingIndex = DataTypeUtils.findFieldIndex(input.getDataFields(), embeddingColumnName);
        int scoreIndex = DataTypeUtils.findFieldIndex(input.getDataFields(), scoreColumnName);
        if (embeddingIndex == -1) {
            throw new IllegalArgumentException("embeddingColumnName not found: " + embeddingColumnName);
        }
        if (scoreIndex == -1) {
            throw new IllegalArgumentException("scoreColumnName not found: " + scoreColumnName);
        }

        List<Object[]> rows = eligibleRows(input, embeddingIndex, scoreIndex);

        int itemSize = rows.size();
        if (itemSize == 0) {
            return new CacheTable(
                    input.getTableName() + "_dpp",
                    Linq4j.asEnumerable(new ArrayList<>()),
                    input.getDataFields()
            );
        }

        double[] rawScores = extractScores(rows, scoreIndex);
        double[][] embeddings = extractEmbeddings(rows, embeddingIndex);

        // Clip negative scores and apply exponential transform: exp(alpha * r)
        // where alpha = theta / (2 * (1 - theta)), theta in [0, 1)
        double[] scores = expTransform(rawScores, thetaVal);

        // Build kernel matrix: L = Diag(scores) * S * Diag(scores)
        // where S[i][j] = (1 + dot(embeddings[i], embeddings[j])) / 2
        // to ensure similarity in [0, 1]
        double[][] kernelData = buildKernelMatrix(embeddings, scores, itemSize);

        // Run DPP greedy MAP inference
        List<Integer> selectedIndices = dppGreedyMap(kernelData, maxLengthVal, itemSize);

        // Build result
        List<Object[]> resultRows = new ArrayList<>();
        for (int idx : selectedIndices) {
            resultRows.add(rows.get(idx));
        }

        return new CacheTable(
                input.getTableName() + "_dpp",
                Linq4j.asEnumerable(resultRows),
                input.getDataFields()
        );
    }

    private static List<Object[]> eligibleRows(
            CacheTable input,
            int embeddingIndex,
            int scoreIndex) {
        List<Object[]> rows = new ArrayList<>();
        Enumerable<Object[]> enumerable = input.scan(null);
        if (enumerable == null) {
            return rows;
        }
        for (Object[] row : enumerable) {
            if (row[scoreIndex] != null && row[embeddingIndex] != null) {
                rows.add(row);
            }
        }
        return rows;
    }

    private static double[] extractScores(List<Object[]> rows, int scoreIndex) {
        double[] scores = new double[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            Object value = rows.get(i)[scoreIndex];
            double score = value instanceof Number
                    ? ((Number) value).doubleValue()
                    : Double.parseDouble(value.toString());
            if (!Double.isFinite(score)) {
                throw new IllegalArgumentException("score must be finite at row " + i);
            }
            scores[i] = score;
        }
        return scores;
    }

    private static double[][] extractEmbeddings(List<Object[]> rows, int embeddingIndex) {
        double[][] embeddings = new double[rows.size()][];
        int expectedDimension = -1;
        for (int i = 0; i < rows.size(); i++) {
            // Normalization is in-place, so always work on an owned copy. In particular,
            // toDoubleArray returns the original reference when the input is a double[].
            double[] vector = DataTransformUtils.toDoubleArray(rows.get(i)[embeddingIndex]).clone();
            if (expectedDimension < 0) {
                expectedDimension = vector.length;
            } else if (vector.length != expectedDimension) {
                throw new IllegalArgumentException(
                        "Embedding dimension mismatch: expected " + expectedDimension
                                + " but got " + vector.length + " at row " + i);
            }
            for (double value : vector) {
                if (!Double.isFinite(value)) {
                    throw new IllegalArgumentException("embedding must be finite at row " + i);
                }
            }
            DataTransformUtils.l2Normalize(vector);
            embeddings[i] = vector;
        }
        return embeddings;
    }

    /**
     * Exponential transform for relevance scores.
     * score_i = exp(alpha * r_i), where alpha = theta / (2 * (1 - theta)).
     * theta in [0, 1): controls relevance-diversity trade-off.
     * - theta close to 1: alpha large, relevance dominates.
     * - theta close to 0: alpha small, diversity dominates.
     * Negative scores are clipped to a small positive value (1e-10).
     */
    private static double[] expTransform(double[] rawScores, double theta) {
        double alpha = theta / (2.0 * (1.0 - theta));
        double[] logScores = new double[rawScores.length];
        double maxLogScore = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < rawScores.length; i++) {
            double r = rawScores[i];
            if (r < 0) {
                r = 1e-10;
            }
            double logScore = alpha * r;
            if (!Double.isFinite(logScore)) {
                throw new IllegalArgumentException(
                        "score transform overflow at row " + i + "; reduce theta or score magnitude");
            }
            logScores[i] = logScore;
            maxLogScore = Math.max(maxLogScore, logScore);
        }

        // A common scale factor does not change fixed-cardinality DPP ranking. Apply one only
        // when needed, leaving enough headroom for score[i] * similarity * score[j].
        double logScale = Math.max(0.0, maxLogScore - MAX_LOG_KERNEL_FACTOR);
        double[] scores = new double[rawScores.length];
        for (int i = 0; i < logScores.length; i++) {
            scores[i] = Math.exp(logScores[i] - logScale);
        }
        return scores;
    }

    /**
     * Build kernel matrix L = Diag(scores) * S * Diag(scores).
     * S[i][j] = (1 + dot(embeddings[i], embeddings[j])) / 2 to ensure similarity in [0, 1].
     * <p>
     * Optimizations:
     * - Directly writes embedding data into EJML's underlying flat array to avoid per-element set() overhead.
     * - Merges similarity transform and diagonal scaling into a single N² pass over the raw result data.
     */
    private static double[][] buildKernelMatrix(
            double[][] embeddings,
            double[] scores,
            int itemSize) {
        int dim = embeddings[0].length;

        // Build embedding matrix (itemSize x dim) by writing directly into EJML's flat array
        DMatrixRMaj embData = new DMatrixRMaj(itemSize, dim);
        double[] embFlat = embData.data;
        for (int i = 0; i < itemSize; i++) {
            System.arraycopy(embeddings[i], 0, embFlat, i * dim, dim);
        }

        // S = E * E^T (cosine similarity since embeddings are L2 normalized)
        DMatrixRMaj simData = new DMatrixRMaj(itemSize, itemSize);
        org.ejml.dense.row.mult.MatrixMatrixMult_DDRM.multTransB(embData, embData, simData);

        // Single pass: transform S = (1 + cos_sim) / 2, then L = Diag(scores) * S * Diag(scores)
        double[] simFlat = simData.data;
        double[][] kernelData = new double[itemSize][itemSize];
        for (int i = 0; i < itemSize; i++) {
            double si = scores[i];
            int rowOffset = i * itemSize;
            for (int j = 0; j < itemSize; j++) {
                double s = (1.0 + simFlat[rowOffset + j]) / 2.0;
                kernelData[i][j] = si * s * scores[j];
            }
        }

        return kernelData;
    }

    /**
     * Fast greedy DPP MAP inference algorithm.
     * <p>
     * Implements the algorithm from Figure 2 of the Hulu paper.
     * Time complexity: O(maxLength^2 * itemSize)
     * <p>
     * Uses raw double[][] for the kernel matrix and Cholesky factor
     * to avoid per-element accessor overhead in the inner loop.
     */
    private static List<Integer> dppGreedyMap(
            double[][] kernelData,
            int maxLength,
            int itemSize) {
        double epsilon = 1e-10;

        if (maxLength > itemSize) {
            maxLength = itemSize;
        }

        // cis: Cholesky factor rows, stored as a (maxLength x itemSize) matrix
        double[][] cis = new double[maxLength][itemSize];
        // di2s: squared diagonal elements of the remaining Cholesky factor
        double[] di2s = new double[itemSize];
        for (int i = 0; i < itemSize; i++) {
            di2s[i] = kernelData[i][i];
        }

        // Persistent selected flags, updated incrementally
        boolean[] selected = new boolean[itemSize];

        List<Integer> selectedItems = new ArrayList<>();

        // Select the first item with the largest diagonal value
        int selectedItem = argMax(di2s, selected);
        selectedItems.add(selectedItem);
        selected[selectedItem] = true;

        while (selectedItems.size() < maxLength) {
            int k = selectedItems.size() - 1;
            double diOptimal = Math.sqrt(Math.max(di2s[selectedItem], epsilon));

            // Compute eis = (L[selectedItem, :] - cis[:k, :].T * cis[:k, selectedItem]) / diOptimal
            double[] eis = cis[k]; // reuse pre-allocated row
            double[] kernelRow = kernelData[selectedItem];
            for (int j = 0; j < itemSize; j++) {
                double val = kernelRow[j];
                for (int p = 0; p < k; p++) {
                    val -= cis[p][selectedItem] * cis[p][j];
                }
                eis[j] = val / diOptimal;
            }

            // Update di2s: di2s -= eis^2, floor at 0 to avoid numerical issues
            for (int j = 0; j < itemSize; j++) {
                di2s[j] = Math.max(0, di2s[j] - eis[j] * eis[j]);
            }

            // Find next best item
            selectedItem = argMax(di2s, selected);
            selectedItems.add(selectedItem);
            selected[selectedItem] = true;
        }

        return selectedItems;
    }

    /**
     * Find the index of the maximum value in the array, excluding already selected indices.
     */
    private static int argMax(double[] arr, boolean[] excluded) {
        int bestIdx = -1;
        double bestVal = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < arr.length; i++) {
            if (excluded != null && excluded[i]) {
                continue;
            }
            if (arr[i] > bestVal) {
                bestVal = arr[i];
                bestIdx = i;
            }
        }
        return bestIdx;
    }
}
