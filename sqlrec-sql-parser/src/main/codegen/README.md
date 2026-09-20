# SQLRec parser code generation

This directory contains only grammar owned by SQLRec.

- `config.fmpp` loads Calcite's published parser defaults and the SQLRec TDD file.
- `data/Parser.tdd` declares SQLRec tokens and parser entry methods.
- `includes/sqlRecIdentifier.ftl` implements SQLRec statements and the small type/option
  helpers they require.

Standard Flink SQL is parsed by `FlinkSqlParserImpl` from the `flink-sql-parser`
dependency. Do not copy Flink's `Parser.tdd`, `parserImpls.ftl`, or generated parser into
this module, and do not generate classes in an `org.apache.flink.*` package.

The generated SQLRec parser is
`com.sqlrec.sql.parser.impl.SqlRecParserImpl`. `SqlRecSqlParser` uses a parser chain and the
`SqlRecStatement` marker to select statement ownership without maintaining a second keyword table.
