package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class SqlIfCache extends SqlCall implements SqlRecStatement {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("IF", SqlKind.OTHER);

    private final boolean timein;
    private final SqlNode condition;
    private final SqlNode thenClause;
    private final SqlNode elseClause;

    public SqlIfCache(SqlParserPos pos, boolean timein, SqlNode condition, SqlNode thenClause, SqlNode elseClause) {
        super(pos);
        this.timein = timein;
        this.condition = condition;
        this.thenClause = thenClause;
        this.elseClause = elseClause;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> operands = new ArrayList<>();
        if (condition != null) {
            operands.add(condition);
        }
        if (thenClause != null) {
            operands.add(thenClause);
        }
        if (elseClause != null) {
            operands.add(elseClause);
        }
        return List.copyOf(operands);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("IF");
        if (timein) {
            writer.keyword("TIMEIN");
        }
        if (condition != null) {
            writer.literal("(");
            condition.unparse(writer, leftPrec, rightPrec);
            writer.literal(")");
        }
        writer.keyword("THEN");
        writer.literal("(");
        thenClause.unparse(writer, leftPrec, rightPrec);
        writer.literal(")");
        if (elseClause != null) {
            writer.keyword("ELSE");
            writer.literal("(");
            elseClause.unparse(writer, leftPrec, rightPrec);
            writer.literal(")");
        }
    }

    public SqlNode getCondition() {
        return condition;
    }

    public SqlNode getThenClause() {
        return thenClause;
    }

    public SqlNode getElseClause() {
        return elseClause;
    }

    public boolean isTimein() {
        return timein;
    }
}
