SqlCache SqlCache() :
{
    SqlIdentifier tableName = null;
    SqlCallSqlFunction callSqlFunction = null;
    SqlNode select = null;
}
{
    <CACHE> <TABLE>
    tableName = SimpleIdentifier()
    <AS>
    (
        <CALL>
        callSqlFunction = GetCallSqlFunction()
    |
        select = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    )
    {
        return new SqlCache(getPos(), tableName, select, callSqlFunction);
    }
}

SqlAssert SqlAssert() :
{
    SqlNode select = null;
}
{
    <ASSERT>
    select = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlAssert(getPos(), select);
    }
}

SqlCallSqlFunction SqlCallSqlFunction() :
{
    SqlCallSqlFunction callSqlFunction = null;
}
{
    <CALL>
    callSqlFunction = GetCallSqlFunction()
    {
        return callSqlFunction;
    }
}


SqlCallSqlFunction GetCallSqlFunction() :
{
    SqlGetVariable funcNameVariable = null;
    SqlIdentifier funcName = null;
    List<SqlNode> inputList = new ArrayList<SqlNode>();
    SqlIdentifier likeTableName = null;
    SqlNode likeFunctionName = null;
    boolean isAsync = false;
    SqlNode partitionBy = null;
    SqlNode partitionSize = null;
}
{
    (
        funcNameVariable = SqlGetVariable()
    |
        funcName = SimpleIdentifier()
    )
    <LPAREN>
    [
        AddCallSqlFunction(inputList)
        ( <COMMA> AddCallSqlFunction(inputList) )*
    ]
    <RPAREN>
    [
        <LIKE>
        (
            <FUNCTION>
            likeFunctionName = StringLiteral()
        |
            likeTableName = SimpleIdentifier()
        )
    ]
    [
        <PARTITION> <BY>
        partitionBy = SimpleIdentifier()
        <SIZE>
        (
            partitionSize = Literal()
        |
            partitionSize = SqlGetVariable()
        )
    ]
    [
        <ASYNC> { isAsync = true; }
    ]
    {
        return new SqlCallSqlFunction(getPos(), funcName, funcNameVariable, inputList, likeTableName, likeFunctionName, isAsync, partitionBy, partitionSize);
    }
}

void AddCallSqlFunction(List<SqlNode> list) :
{
    final SqlNode input;
}
{
    (
        input = SimpleIdentifier()
    |
        input = SqlGetVariable()
    |
        input = StringLiteral()
    )
    { list.add(input); }
}

SqlGetVariable SqlGetVariable() :
{
    SqlNode variableName = null;
    SqlNode defaultValue = null;
}
{
    (
        <GET>
        <LPAREN>
        variableName = StringLiteral()
        <RPAREN>
        {
            return new SqlGetVariable(getPos(), variableName);
        }
    |
        <GET_OR_DEFAULT>
        <LPAREN>
        variableName = StringLiteral()
        <COMMA>
        defaultValue = StringLiteral()
        <RPAREN>
        {
            return new SqlGetVariable(getPos(), variableName, defaultValue);
        }
    )
}

SqlDefineInputTable SqlDefineInputTable():
{
    SqlIdentifier tableName = null;
    SqlIdentifier likeTable = null;
    List<SqlIdentifier> columnList = new ArrayList<SqlIdentifier>();
    List<SqlTypeNameSpec> columnTypeList = new ArrayList<SqlTypeNameSpec>();
}
{
    <DEFINE> <INPUT> <TABLE>
    tableName = SimpleIdentifier()
    (
        <LIKE> likeTable = CompoundIdentifier()
        {
            return new SqlDefineInputTable(getPos(), tableName, likeTable);
        }
    |
        <LPAREN>
            AddDefineInputTable(columnList, columnTypeList)
            ( <COMMA> AddDefineInputTable(columnList, columnTypeList) )*
        <RPAREN>
        {
            return new SqlDefineInputTable(getPos(), tableName, columnList, columnTypeList);
        }
    )
}

void AddDefineInputTable(List<SqlIdentifier> columnList, List<SqlTypeNameSpec> columnTypeList):
{
    final SqlIdentifier columnName;
    final SqlTypeNameSpec columnType;
}
{
    columnName = SimpleIdentifier()
    columnType = TypeName()
    {
        columnList.add(columnName);
        columnTypeList.add(columnType);
    }
}

SqlCall SqlCreateResource() :
{
    SqlIdentifier apiName = null;
    SqlIdentifier funcName = null;
    boolean orReplace = false;
}
{
    <CREATE>
    [
        <OR> <REPLACE> { orReplace = true; }
    ]
    (
        <API>
        apiName = SimpleIdentifier()
        <WITH>
        funcName = SimpleIdentifier()
        {
            return new SqlCreateApi(getPos(), apiName, funcName, orReplace);
        }
    |
        <SQL>
        <FUNCTION>
        funcName = SimpleIdentifier()
        {
            return new SqlCreateSqlFunction(getPos(), funcName, orReplace);
        }
    )
}

SqlReturn SqlReturn() :
{
    SqlIdentifier tableName = null;
    SqlNode select = null;
    SqlCallSqlFunction callSqlFunction = null;
}
{
    <RETURN>
    [
        (
            <CALL>
            callSqlFunction = GetCallSqlFunction()
        |
            LOOKAHEAD(<SELECT>)
            select = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        |
            tableName = SimpleIdentifier()
        )
    ]
    {
        return new SqlReturn(getPos(), tableName, select, callSqlFunction);
    }
}

SqlFlush SqlFlush() :
{
}
{
    <FLUSH>
    {
        return new SqlFlush(getPos());
    }
}

SqlShowSqlFunction SqlShowSqlFunction() :
{
}
{
    <SHOW>
    <SQL>
    <FUNCTIONS>
    {
        return new SqlShowSqlFunction(getPos());
    }
}

SqlCall SqlDescribeStatement() :
{
    SqlIdentifier name = null;
    SqlNode checkpoint = null;
    boolean formatted = false;
}
{
    ( <DESCRIBE> | <DESC> )
    [ <FORMATTED> { formatted = true; } ]
    (
        <MODEL>
        name = SimpleIdentifier()
        [
            <CHECKPOINT> <EQ>
            checkpoint = StringLiteral()
        ]
        {
            return new SqlShowCreateModel(getPos(), name, checkpoint, formatted);
        }
    |
        <SERVICE>
        name = SimpleIdentifier()
        {
            return new SqlShowCreateService(getPos(), name, formatted);
        }
    |
        <SQL>
        <FUNCTION>
        name = SimpleIdentifier()
        {
            return new SqlShowCreateSqlFunction(getPos(), name);
        }
    |
        <API>
        name = SimpleIdentifier()
        {
            return new SqlShowCreateApi(getPos(), name);
        }
    )
}

SqlShowApi SqlShowApi() :
{
}
{
    <SHOW>
    <APIS>
    {
        return new SqlShowApi(getPos());
    }
}

SqlShowModel SqlShowModel() :
{
}
{
    <SHOW>
    <MODELS>
    {
        return new SqlShowModel(getPos());
    }
}

SqlShowCheckpoint SqlShowCheckpoint() :
{
    SqlIdentifier modelName = null;
}
{
    <SHOW>
    <CHECKPOINTS>
    modelName = SimpleIdentifier()
    {
        return new SqlShowCheckpoint(getPos(), modelName);
    }
}

SqlShowService SqlShowService() :
{
}
{
    <SHOW>
    <SERVICES>
    {
        return new SqlShowService(getPos());
    }
}

SqlNode SqlCreateModel() : {
    SqlParserPos startPos;
    boolean ifNotExists = false;
    SqlIdentifier modelName;
    SqlNodeList columnList = SqlNodeList.EMPTY;
    SqlNodeList propertyList = SqlNodeList.EMPTY;
    List<SqlNode> columns = new ArrayList<SqlNode>();
    SqlNode column;
    Span columnSpan;
}
{
    <CREATE>
    <MODEL>
    { startPos = getPos(); }

    ifNotExists = IfNotExistsOpt()

    modelName = SimpleIdentifier()
    [
        <LPAREN> { columnSpan = span(); }
        column = SqlRecModelColumn() { columns.add(column); }
        (
            <COMMA> column = SqlRecModelColumn() { columns.add(column); }
        )*
        {
            columnList = new SqlNodeList(columns, columnSpan.end(this));
        }
        <RPAREN>
    ]
    [
        <WITH>
        propertyList = SqlRecProperties()
    ]
    {
        return new com.sqlrec.sql.parser.SqlCreateModel(
            startPos.plus(getPos()),
            modelName,
            columnList,
            propertyList,
            ifNotExists);
    }
}

SqlNode SqlDropModel() :
{
    SqlParserPos startPos;
    SqlIdentifier modelName = null;
    boolean ifExists = false;
}
{
    <DROP>
    <MODEL>
    { startPos = getPos(); }

    ifExists = IfExistsOpt()

    modelName = SimpleIdentifier()
    {
        return new com.sqlrec.sql.parser.SqlDropModel(startPos.plus(getPos()), modelName, ifExists);
    }
}

SqlNode SqlTrainModel() : {
    SqlParserPos pos;
    SqlIdentifier modelName;
    SqlNode checkpoint = null;
    SqlIdentifier dataSource = null;
    SqlNode whereCondition = null;
    SqlNode existingCheckpoint = null;
    SqlNodeList propertyList = null;
}
{
    <TRAIN>
    <MODEL>
    pos = getPos()
    modelName = SimpleIdentifier()
    <CHECKPOINT> <EQ>
    checkpoint = StringLiteral()
    [
        <ON>
        dataSource = CompoundIdentifier()
        [
            <WHERE>
            whereCondition = Expression(ExprContext.ACCEPT_NON_QUERY)
        ]
    ]
    [
        <FROM>
        existingCheckpoint = StringLiteral()
    ]
    [
        <WITH>
        propertyList = SqlRecProperties()
    ]
    {
        return new com.sqlrec.sql.parser.SqlTrainModel(
            pos.plus(getPos()),
            modelName,
            checkpoint,
            dataSource,
            whereCondition,
            existingCheckpoint,
            propertyList);
    }
}

SqlNode SqlExportModel() : {
    SqlParserPos pos;
    SqlIdentifier modelName;
    SqlNode checkpoint = null;
    SqlIdentifier dataSource = null;
    SqlNode whereCondition = null;
    SqlNodeList propertyList = null;
}
{
    <EXPORT>
    <MODEL>
    pos = getPos()
    modelName = SimpleIdentifier()
    <CHECKPOINT> <EQ>
    checkpoint = StringLiteral()
    [
        <ON>
        dataSource = CompoundIdentifier()
    ]
    [
        <WHERE>
        whereCondition = Expression(ExprContext.ACCEPT_NON_QUERY)
    ]
    [
        <WITH>
        propertyList = SqlRecProperties()
    ]
    {
        return new com.sqlrec.sql.parser.SqlExportModel(
            pos.plus(getPos()),
            modelName,
            checkpoint,
            dataSource,
            whereCondition,
            propertyList);
    }
}

SqlNode SqlCreateService() : {
    SqlParserPos pos;
    boolean ifNotExists = false;
    SqlIdentifier serviceName;
    SqlIdentifier modelName;
    SqlNode checkpoint = null;
    SqlNodeList propertyList = null;
}
{
    <CREATE>
    <SERVICE>
    pos = getPos()
    ifNotExists = IfNotExistsOpt()
    serviceName = SimpleIdentifier()
    <ON>
    <MODEL>
    modelName = SimpleIdentifier()
    [
        <CHECKPOINT> <EQ>
        checkpoint = StringLiteral()
    ]
    [
        <WITH>
        propertyList = SqlRecProperties()
    ]
    {
        return new com.sqlrec.sql.parser.SqlCreateService(
            pos.plus(getPos()),
            serviceName,
            modelName,
            checkpoint,
            propertyList,
            ifNotExists);
    }
}

SqlDropService SqlDropService() :
{
    SqlParserPos startPos;
    SqlIdentifier serviceName = null;
    boolean ifExists = false;
}
{
    <DROP>
    <SERVICE>
    { startPos = getPos(); }

    ifExists = IfExistsOpt()

    serviceName = SimpleIdentifier()
    {
        return new SqlDropService(startPos.plus(getPos()), serviceName, ifExists);
    }
}

SqlDropSqlFunction SqlDropSqlFunction() :
{
    SqlParserPos startPos;
    SqlIdentifier funcName = null;
    boolean ifExists = false;
}
{
    <DROP>
    <SQL>
    <FUNCTION>
    { startPos = getPos(); }

    ifExists = IfExistsOpt()

    funcName = SimpleIdentifier()
    {
        return new SqlDropSqlFunction(startPos.plus(getPos()), funcName, ifExists);
    }
}

SqlDropApi SqlDropApi() :
{
    SqlParserPos startPos;
    SqlIdentifier apiName = null;
    boolean ifExists = false;
}
{
    <DROP>
    <API>
    { startPos = getPos(); }

    ifExists = IfExistsOpt()

    apiName = SimpleIdentifier()
    {
        return new SqlDropApi(startPos.plus(getPos()), apiName, ifExists);
    }
}

SqlAlterModelDropCheckpoint SqlAlterModelDropCheckpoint() :
{
    SqlParserPos startPos;
    SqlIdentifier modelName = null;
    SqlNode checkpointName = null;
    boolean ifExists = false;
}
{
    <ALTER>
    <MODEL>
    { startPos = getPos(); }

    modelName = SimpleIdentifier()
    <DROP>
    ifExists = IfExistsOpt()
    <CHECKPOINT>
    <EQ>
    checkpointName = StringLiteral()
    {
        return new SqlAlterModelDropCheckpoint(startPos.plus(getPos()), modelName, checkpointName, ifExists);
    }
}

SqlIfCache SqlIfCache() :
{
    SqlParserPos startPos;
    boolean timein = false;
    SqlNode condition = null;
    SqlNode thenClause = null;
    SqlNode elseClause = null;
}
{
    <IF>
    { startPos = getPos(); }
    [
        <TIMEIN>
        { timein = true; }
    ]
    <LPAREN>
    condition = QueryOrExpr(ExprContext.ACCEPT_QUERY)
    <RPAREN>
    <THEN>
    <LPAREN>
    thenClause = SqlStmt()
    <RPAREN>
    [
        <ELSE>
        <LPAREN>
        elseClause = SqlStmt()
        <RPAREN>
    ]
    {
        return new SqlIfCache(startPos.plus(getPos()), timein, condition, thenClause, elseClause);
    }
}

/** Parses the regular columns supported by CREATE MODEL. */
SqlRecColumnDeclaration SqlRecModelColumn() :
{
    SqlIdentifier name;
    SqlDataTypeSpec dataType;
    SqlParserPos pos;
}
{
    name = SimpleIdentifier() { pos = getPos(); }
    dataType = DataType()
    {
        return new SqlRecColumnDeclaration(pos.plus(getPos()), name, dataType);
    }
}

/** Parses SQLRec resource options without depending on Flink's grammar helpers. */
SqlNodeList SqlRecProperties() :
{
    final List<SqlNode> properties = new ArrayList<SqlNode>();
    SqlRecOption property;
    final Span span;
}
{
    <LPAREN> { span = span(); }
    [
        property = SqlRecOption() { properties.add(property); }
        (
            <COMMA> property = SqlRecOption() { properties.add(property); }
        )*
    ]
    <RPAREN>
    {
        return new SqlNodeList(properties, span.end(this));
    }
}

SqlRecOption SqlRecOption() :
{
    SqlNode key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = StringLiteral() { pos = getPos(); }
    <EQ>
    value = StringLiteral()
    {
        return new SqlRecOption(pos.plus(getPos()), key, value);
    }
}

/** ARRAY&lt;T&gt; and MULTISET&lt;T&gt; syntax used by SQLRec schemas. */
SqlTypeNameSpec SqlRecCollectionTypeName() :
{
    SqlTypeName collectionType;
    SqlTypeNameSpec elementType;
    boolean elementNullable = true;
    SqlParserPos pos;
}
{
    (
        <ARRAY> { collectionType = SqlTypeName.ARRAY; pos = getPos(); }
    |
        <MULTISET> { collectionType = SqlTypeName.MULTISET; pos = getPos(); }
    )
    <LT>
    elementType = TypeName()
    [
        <NOT> <NULL> { elementNullable = false; }
    |
        <NULL> { elementNullable = true; }
    ]
    <GT>
    {
        return new SqlRecCollectionTypeNameSpec(
            elementType,
            collectionType,
            elementNullable,
            pos.plus(getPos()));
    }
}

/** STRING and BYTES aliases used by SQLRec schemas. */
SqlTypeNameSpec SqlRecAliasTypeName() :
{
    SqlTypeName typeName;
    String alias;
}
{
    (
        <STRING>
        {
            typeName = SqlTypeName.VARCHAR;
            alias = token.image;
        }
    |
        <BYTES>
        {
            typeName = SqlTypeName.VARBINARY;
            alias = token.image;
        }
    )
    {
        return new SqlAlienSystemTypeNameSpec(alias, typeName, Integer.MAX_VALUE, getPos());
    }
}

/**
 * Keeps SET usable inside IF while standard top-level SET remains owned by the
 * dependency-provided Flink parser.
 */
SqlNode SqlRecSet() :
{
    Span span;
    SqlNode key = null;
    SqlNode value = null;
}
{
    <SET> { span = span(); }
    [
        key = StringLiteral()
        <EQ>
        value = StringLiteral()
    ]
    {
        if (key == null) {
            return new SqlSet(span.end(this));
        }
        return new SqlSet(span.end(this), key, value);
    }
}

boolean IfExistsOpt() :
{
}
{
    (
        LOOKAHEAD(2)
        <IF> <EXISTS> { return true; }
    |
        { return false; }
    )
}

boolean IfNotExistsOpt() :
{
}
{
    (
        LOOKAHEAD(3)
        <IF> <NOT> <EXISTS> { return true; }
    |
        { return false; }
    )
}
