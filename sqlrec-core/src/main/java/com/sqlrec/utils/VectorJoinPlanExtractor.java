package com.sqlrec.utils;

import com.sqlrec.common.schema.VectorSearchable;
import com.sqlrec.common.utils.FilterUtils;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Extracts the supported vector lookup shape from a Calcite logical plan. */
public final class VectorJoinPlanExtractor {
    private static final String INNER_PRODUCT_FUNCTION = "ip";

    private VectorJoinPlanExtractor() {
    }

    public static Optional<VectorLookupPlan> extract(
            LogicalSort sort,
            LogicalProject project,
            LogicalFilter filter,
            LogicalJoin join) {
        if (join.getJoinType() != JoinRelType.INNER || !NodeUtils.isTrueCondition(join)) {
            return Optional.empty();
        }
        // Only a plain scan is safe. Volcano may wrap it in a RelSubset, but this
        // deliberately does not walk through filters, projects, or other operators.
        LogicalTableScan rightScan = findLogicalTableScan(join.getRight());
        if (rightScan == null) {
            return Optional.empty();
        }

        RelOptTable rightTable = rightScan.getTable();
        if (rightTable.unwrap(VectorSearchable.class) == null || sort.offset != null) {
            return Optional.empty();
        }

        List<RelFieldCollation> sortFields = sort.getCollation().getFieldCollations();
        if (sortFields.size() != 1) {
            return Optional.empty();
        }
        int sortedProjectIndex = sortFields.get(0).getFieldIndex();
        if (sortedProjectIndex < 0 || sortedProjectIndex >= project.getProjects().size()) {
            return Optional.empty();
        }

        RexNode sortedExpression = project.getProjects().get(sortedProjectIndex);
        if (!(sortedExpression instanceof RexCall) || !isInnerProductCall((RexCall) sortedExpression)) {
            return Optional.empty();
        }

        RexCall scoreCall = (RexCall) sortedExpression;
        EmbeddingColumns embeddingColumns = extractEmbeddingColumns(scoreCall, join);
        if (embeddingColumns == null) {
            return Optional.empty();
        }

        int topKPerLeftRow = 0;
        if (sort.fetch != null) {
            if (!(sort.fetch instanceof RexLiteral)) {
                return Optional.empty();
            }
            topKPerLeftRow = RexLiteral.intValue(sort.fetch);
        }

        int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        int rightFieldCount = join.getRight().getRowType().getFieldCount();
        FilterSplit filterSplit = splitFilter(
                filter == null ? null : filter.getCondition(),
                leftFieldCount,
                rightFieldCount,
                join.getCluster().getRexBuilder());
        if (!filterSplit.isSupported()) {
            return Optional.empty();
        }

        return Optional.of(new VectorLookupPlan(
                rightScan,
                embeddingColumns.leftIndex,
                embeddingColumns.rightName,
                topKPerLeftRow,
                filterSplit.getLeftFilter(),
                filterSplit.getPushedFilter(),
                scoreCall,
                project.getProjects(),
                project.getRowType(),
                sort.getCollation()));
    }

    private static LogicalTableScan findLogicalTableScan(RelNode node) {
        if (node instanceof LogicalTableScan) {
            return (LogicalTableScan) node;
        }
        if (node instanceof RelSubset) {
            for (RelNode equivalent : ((RelSubset) node).getRelList()) {
                if (equivalent instanceof LogicalTableScan) {
                    return (LogicalTableScan) equivalent;
                }
            }
        }
        return null;
    }

    static FilterSplit splitFilter(
            RexNode condition,
            int leftFieldCount,
            int rightFieldCount,
            RexBuilder rexBuilder) {
        if (condition == null) {
            return FilterSplit.supported(null, null);
        }

        int totalFieldCount = leftFieldCount + rightFieldCount;
        List<RexNode> leftFilters = new ArrayList<>();
        List<RexNode> pushedFilters = new ArrayList<>();
        for (RexNode conjunct : RelOptUtil.conjunctions(condition)) {
            ImmutableBitSet inputs = RelOptUtil.InputFinder.bits(conjunct);
            if (inputs.length() > totalFieldCount) {
                return FilterSplit.unsupported();
            }

            boolean referencesRight = inputs.nextSetBit(leftFieldCount) >= 0;
            if (!referencesRight) {
                leftFilters.add(conjunct);
                continue;
            }

            if (!FilterUtils.canBuildMilvusJoinFilter(
                    conjunct, leftFieldCount, rightFieldCount)) {
                return FilterSplit.unsupported();
            }
            pushedFilters.add(conjunct);
        }

        return FilterSplit.supported(
                composeConjunction(rexBuilder, leftFilters),
                composeConjunction(rexBuilder, pushedFilters));
    }

    private static RexNode composeConjunction(RexBuilder rexBuilder, List<RexNode> conditions) {
        return conditions.isEmpty() ? null : RexUtil.composeConjunction(rexBuilder, conditions);
    }

    private static EmbeddingColumns extractEmbeddingColumns(RexCall call, LogicalJoin join) {
        if (call.getOperands().size() != 2
                || !(call.getOperands().get(0) instanceof RexInputRef)
                || !(call.getOperands().get(1) instanceof RexInputRef)) {
            return null;
        }

        int first = ((RexInputRef) call.getOperands().get(0)).getIndex();
        int second = ((RexInputRef) call.getOperands().get(1)).getIndex();
        int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        int rightFieldCount = join.getRight().getRowType().getFieldCount();

        int leftIndex;
        int rightIndex;
        if (first < leftFieldCount && second >= leftFieldCount) {
            leftIndex = first;
            rightIndex = second - leftFieldCount;
        } else if (second < leftFieldCount && first >= leftFieldCount) {
            leftIndex = second;
            rightIndex = first - leftFieldCount;
        } else {
            return null;
        }
        if (leftIndex < 0 || rightIndex < 0 || rightIndex >= rightFieldCount) {
            return null;
        }

        String rightName = join.getRight().getRowType().getFieldNames().get(rightIndex);
        return new EmbeddingColumns(leftIndex, rightName);
    }

    private static boolean isInnerProductCall(RexCall call) {
        return call.getOperator().getName().equalsIgnoreCase(INNER_PRODUCT_FUNCTION);
    }

    private static final class EmbeddingColumns {
        private final int leftIndex;
        private final String rightName;

        private EmbeddingColumns(int leftIndex, String rightName) {
            this.leftIndex = leftIndex;
            this.rightName = rightName;
        }
    }

    public static final class FilterSplit {
        private final boolean supported;
        private final RexNode leftFilter;
        private final RexNode pushedFilter;

        private FilterSplit(boolean supported, RexNode leftFilter, RexNode pushedFilter) {
            this.supported = supported;
            this.leftFilter = leftFilter;
            this.pushedFilter = pushedFilter;
        }

        private static FilterSplit supported(RexNode leftFilter, RexNode pushedFilter) {
            return new FilterSplit(true, leftFilter, pushedFilter);
        }

        private static FilterSplit unsupported() {
            return new FilterSplit(false, null, null);
        }

        public boolean isSupported() {
            return supported;
        }

        public RexNode getLeftFilter() {
            return leftFilter;
        }

        public RexNode getPushedFilter() {
            return pushedFilter;
        }
    }

    public static final class VectorLookupPlan {
        private final LogicalTableScan rightScan;
        private final int leftEmbeddingIndex;
        private final String rightEmbeddingField;
        private final int topKPerLeftRow;
        private final RexNode leftFilter;
        private final RexNode pushedFilter;
        private final RexCall scoreCall;
        private final List<RexNode> projects;
        private final RelDataType projectRowType;
        private final RelCollation collation;

        private VectorLookupPlan(
                LogicalTableScan rightScan,
                int leftEmbeddingIndex,
                String rightEmbeddingField,
                int topKPerLeftRow,
                RexNode leftFilter,
                RexNode pushedFilter,
                RexCall scoreCall,
                List<RexNode> projects,
                RelDataType projectRowType,
                RelCollation collation) {
            this.rightScan = rightScan;
            this.leftEmbeddingIndex = leftEmbeddingIndex;
            this.rightEmbeddingField = rightEmbeddingField;
            this.topKPerLeftRow = topKPerLeftRow;
            this.leftFilter = leftFilter;
            this.pushedFilter = pushedFilter;
            this.scoreCall = scoreCall;
            this.projects = Collections.unmodifiableList(new ArrayList<>(projects));
            this.projectRowType = projectRowType;
            this.collation = collation;
        }

        public List<RexNode> rewriteProjects(int scoreFieldIndex) {
            RexInputRef scoreRef = new RexInputRef(scoreFieldIndex, scoreCall.getType());
            RexShuttle shuttle = new RexShuttle() {
                @Override
                public RexNode visitCall(RexCall call) {
                    return call.equals(scoreCall) ? scoreRef : super.visitCall(call);
                }
            };

            List<RexNode> rewritten = new ArrayList<>(projects.size());
            for (RexNode project : projects) {
                rewritten.add(project.accept(shuttle));
            }
            return rewritten;
        }

        public int getLeftEmbeddingIndex() {
            return leftEmbeddingIndex;
        }

        public LogicalTableScan getRightScan() {
            return rightScan;
        }

        public String getRightEmbeddingField() {
            return rightEmbeddingField;
        }

        public int getTopKPerLeftRow() {
            return topKPerLeftRow;
        }

        public RexNode getLeftFilter() {
            return leftFilter;
        }

        public RexNode getPushedFilter() {
            return pushedFilter;
        }

        public RelDataType getScoreType() {
            return scoreCall.getType();
        }

        public RelDataType getProjectRowType() {
            return projectRowType;
        }

        public RelCollation getCollation() {
            return collation;
        }
    }
}
