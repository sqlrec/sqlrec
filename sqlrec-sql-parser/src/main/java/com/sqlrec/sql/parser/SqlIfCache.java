package com.sqlrec.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class SqlIfCache extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("IF", SqlKind.OTHER);

    private boolean timein;
    private SqlNode condition;
    private SqlCache thenClause;
    private SqlCache elseClause;

    public SqlIfCache(SqlParserPos pos, boolean timein, SqlNode condition, SqlCache thenClause, SqlCache elseClause) {
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
        return operands;
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

    public SqlCache getThenClause() {
        return thenClause;
    }

    public SqlCache getElseClause() {
        return elseClause;
    }

    public boolean isTimein() {
        return timein;
    }
}
