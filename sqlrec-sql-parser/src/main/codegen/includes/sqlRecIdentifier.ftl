
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
