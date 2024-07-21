
SqlCache SqlCache() :
{
    SqlIdentifier tableName = null;
    SqlIdentifier funcName = null;
    List<SqlIdentifier> inputTableList = new ArrayList<SqlIdentifier>();
    SqlSelect select = null;
}
{
    <CACHE> <TABLE>
    tableName = CompoundTableIdentifier()
    <AS>
    (
        select = SqlSelect()
    |
        funcName = SimpleIdentifier()
        <LPAREN>
        [
            AddCacheFunTable(inputTableList)
            ( <COMMA> AddCacheFunTable(inputTableList) )*
        ]
        <RPAREN>
    )
    {
        return new SqlCache(getPos(), tableName, select, funcName, inputTableList);
    }
}

void AddCacheFunTable(List<SqlIdentifier> list) :
{
    final SqlIdentifier tableName;
}
{
    tableName = SimpleIdentifier()
    { list.add(tableName); }
}

SqlCreateApi SqlCreateApi() :
{
    SqlIdentifier apiName = null;
    SqlIdentifier funcName = null;
}
{
    <CREATE>
    <API>
    apiName = SimpleIdentifier()
    <WITH>
    funcName = SimpleIdentifier()
    {
        return new SqlCreateApi(getPos(), apiName, funcName);
    }
}

SqlCreateSqlFunction SqlCreateSqlFunction() :
{
    SqlIdentifier funcName = null;
}
{
    <CREATE>
    <SQL>
    <FUNCTION>
    funcName = CompoundTableIdentifier()
    {
        return new SqlCreateSqlFunction(getPos(), funcName);
    }
}

SqlReturn SqlReturn() :
{
    SqlIdentifier tableName = null;
}
{
    <RETURN>
    tableName = CompoundTableIdentifier()
    {
        return new SqlReturn(getPos(), tableName);
    }
}