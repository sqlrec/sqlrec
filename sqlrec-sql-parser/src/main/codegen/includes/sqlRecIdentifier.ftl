
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
        select = SqlQueryEof()
    )
    {
        return new SqlCache(getPos(), tableName, select, callSqlFunction);
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
    boolean isAsync = false;
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
        likeTableName = SimpleIdentifier()
    ]
    [
        <ASYNC> { isAsync = true; }
    ]
    {
        return new SqlCallSqlFunction(getPos(), funcName, funcNameVariable, inputList, likeTableName, isAsync);
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
}
{
    <GET>
    <LPAREN>
    variableName = StringLiteral()
    <RPAREN>
    {
        return new SqlGetVariable(getPos(), variableName);
    }
}

SqlDefineInputTable SqlDefineInputTable():
{
    SqlIdentifier tableName = null;
    List<SqlIdentifier> columnList = new ArrayList<SqlIdentifier>();
    List<SqlTypeNameSpec> columnTypeList = new ArrayList<SqlTypeNameSpec>();
}
{
    <DEFINE> <INPUT> <TABLE>
    tableName = SimpleIdentifier()
    <LPAREN>
        AddDefineInputTable(columnList, columnTypeList)
        ( <COMMA> AddDefineInputTable(columnList, columnTypeList) )*
    <RPAREN>
    {
        return new SqlDefineInputTable(getPos(), tableName, columnList, columnTypeList);
    }
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
}
{
    <RETURN>
    [
        tableName = SimpleIdentifier()
    ]
    {
        return new SqlReturn(getPos(), tableName);
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

SqlShowCreateSqlFunction SqlShowCreateSqlFunction() :
{
    SqlIdentifier funcName = null;
}
{
    ( <DESCRIBE> | <DESC> )
    <SQL>
    <FUNCTION>
    funcName = SimpleIdentifier()
    {
        return new SqlShowCreateSqlFunction(getPos(), funcName);
    }
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

SqlShowCreateApi SqlShowCreateApi() :
{
    SqlIdentifier apiName = null;
}
{
    ( <DESCRIBE> | <DESC> )
    <API>
    apiName = SimpleIdentifier()
    {
        return new SqlShowCreateApi(getPos(), apiName);
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

SqlShowCreateModel SqlShowCreateModel() :
{
    SqlIdentifier modelName = null;
}
{
    ( <DESCRIBE> | <DESC> )
    <MODEL>
    modelName = SimpleIdentifier()
    {
        return new SqlShowCreateModel(getPos(), modelName);
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

SqlShowCreateCheckpoint SqlShowCreateCheckpoint() :
{
    SqlIdentifier modelName = null;
    SqlIdentifier checkpointName = null;
}
{
    ( <DESCRIBE> | <DESC> )
    <CHECKPOINT>
    modelName = SimpleIdentifier()
    <DOT>
    checkpointName = SimpleIdentifier()
    {
        return new SqlShowCreateCheckpoint(getPos(), modelName, checkpointName);
    }
}

SqlNode SqlCreateModel() : {
    SqlParserPos startPos;
    boolean ifNotExists = false;
    SqlIdentifier modelName;
    SqlNodeList columnList = SqlNodeList.EMPTY;
    SqlNodeList propertyList = SqlNodeList.EMPTY;
    SqlParserPos pos;
}
{
    <CREATE>
    <MODEL>
    { startPos = getPos(); }

    ifNotExists = IfNotExistsOpt()

    modelName = CompoundIdentifier()
    [
        <LPAREN> { pos = getPos(); TableCreationContext ctx = new TableCreationContext();}
        TableColumn(ctx)
        (
            <COMMA> TableColumn(ctx)
        )*
        {
            pos = pos.plus(getPos());
            columnList = new SqlNodeList(ctx.columnList, pos);
        }
        <RPAREN>
    ]
    [
        <WITH>
        propertyList = TableProperties()
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

    modelName = CompoundIdentifier()
    {
        return new com.sqlrec.sql.parser.SqlDropModel(startPos.plus(getPos()), modelName, ifExists);
    }
}

SqlNode SqlTrainModel() : {
    SqlParserPos pos;
    SqlIdentifier modelName;
    SqlNode checkpoint = null;
    SqlIdentifier dataSource;
    SqlNode whereCondition = null;
    SqlNode existingCheckpoint = null;
    SqlNodeList propertyList = null;
}
{
    <TRAIN>
    <MODEL>
    pos = getPos()
    modelName = CompoundIdentifier()
    <CHECKPOINT> <EQ>
    checkpoint = StringLiteral()
    <ON>
    dataSource = CompoundIdentifier()
    [
        <WHERE>
        whereCondition = Expression(ExprContext.ACCEPT_NON_QUERY)
    ]
    [
        <FROM>
        existingCheckpoint = StringLiteral()
    ]
    [
        <WITH>
        propertyList = TableProperties()
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