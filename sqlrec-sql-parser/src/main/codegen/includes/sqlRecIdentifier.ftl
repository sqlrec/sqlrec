
SqlCache SqlCache() :
{
    SqlIdentifier tableName = null;
    SqlCallSqlFunction callSqlFunction = null;
    SqlSelect select = null;
}
{
    <CACHE> <TABLE>
    tableName = CompoundTableIdentifier()
    <AS>
    (
        select = SqlSelect()
    |
        callSqlFunction = GetCallSqlFunction()
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
    <CALL_SQL_FUNCTION>
    callSqlFunction = GetCallSqlFunction()
    {
        return callSqlFunction;
    }
}


SqlCallSqlFunction GetCallSqlFunction() :
{
    SqlIdentifier funcName = null;
    List<SqlIdentifier> inputList = new ArrayList<SqlIdentifier>();
}
{
    funcName = CompoundTableIdentifier()
    <LPAREN>
    [
        AddCallSqlFunction(inputList)
        ( <COMMA> AddCallSqlFunction(inputList) )*
    ]
    <RPAREN>
    {
        return new SqlCallSqlFunction(getPos(), funcName, inputList);
    }
}

void AddCallSqlFunction(List<SqlIdentifier> list) :
{
    final SqlIdentifier tableName;
}
{
    tableName = SimpleIdentifier()
    { list.add(tableName); }
}

SqlDefineInputTable SqlDefineInputTable():
{
    SqlIdentifier tableName = null;
    List<SqlIdentifier> columnList = new ArrayList<SqlIdentifier>();
    List<SqlTypeNameSpec> columnTypeList = new ArrayList<SqlTypeNameSpec>();
}
{
    <DEFINE> <INPUT> <TABLE>
    tableName = CompoundTableIdentifier()
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

SqlCreateApi SqlCreateApi() :
{
    SqlIdentifier apiName = null;
    SqlIdentifier funcName = null;
    boolean orReplace = false;
}
{
    <CREATE>
    orReplace = OrReplaceOpt()
    <API>
    apiName = SimpleIdentifier()
    <WITH>
    funcName = SimpleIdentifier()
    {
        return new SqlCreateApi(getPos(), apiName, funcName, orReplace);
    }
}

SqlCreateSqlFunction SqlCreateSqlFunction() :
{
    SqlIdentifier funcName = null;
    boolean orReplace = false;
}
{
    <CREATE>
    orReplace = OrReplaceOpt()
    <SQL>
    <FUNCTION>
    funcName = CompoundTableIdentifier()
    {
        return new SqlCreateSqlFunction(getPos(), funcName, orReplace);
    }
}

SqlReturn SqlReturn() :
{
    SqlIdentifier tableName = null;
}
{
    <RETURN>
    [
        tableName = CompoundTableIdentifier()
    ]
    {
        return new SqlReturn(getPos(), tableName);
    }
}

boolean OrReplaceOpt() :
{
}
{
    (
        LOOKAHEAD(2)
        <OR> <REPLACE> { return true; }
    |
        { return false; }
    )
}