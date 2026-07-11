package com.sqlrec.frontend;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.executor.SqlExecutor;
import com.sqlrec.frontend.cli.*;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Local SQL CLI built on JLine + Picocli.
 * <p>
 * Supports:
 * <ul>
 *     <li>-e / -f to execute SQL from the command line or a file</li>
 *     <li>Interactive REPL: multi-line SQL, syntax highlighting, command history</li>
 *     <li>--outputformat table|csv|tsv|json</li>
 * </ul>
 * Press Ctrl+D to exit the REPL.
 */
@Command(
        name = "sqlrec-cli",
        mixinStandardHelpOptions = true,
        versionProvider = SqlRecVersionProvider.class,
        description = "Local SQL CLI for sqlrec, supports -e/-f, output format, interactive REPL with history."
)
public class Cli implements Callable<Integer> {

    private static final String PROMPT1 = "sqlrec> ";
    private static final String PROMPT2 = "......> ";
    private static final String HISTORY_FILE = System.getProperty("user.home", ".") + "/.sqlrec_history";

    @Option(names = {"-e", "--execute"},
            description = "Execute the given SQL string (multiple statements separated by ';').")
    private String inlineSql;

    @Option(names = {"-f", "--file"},
            description = "Execute SQL statements from the given file.")
    private Path sqlFile;

    @Option(names = {"--outputformat", "--format"}, defaultValue = "table",
            description = "Output format: table (default), csv, tsv, json.")
    private String outputFormat;

    /**
     * Active during the interactive REPL; null in batch mode.
     */
    private LineReader reader;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Cli()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        SqlExecutor sqlExecutor = new SqlExecutor();

        if (inlineSql != null) {
            executeStatements(sqlExecutor, inlineSql);
            return 0;
        }
        if (sqlFile != null) {
            String content = Files.readString(sqlFile);
            executeStatements(sqlExecutor, content);
            return 0;
        }

        return runInteractive(sqlExecutor);
    }

    // ===================== Batch / interactive execution =====================

    private void executeStatements(SqlExecutor sqlExecutor, String text) {
        for (String stmt : SqlStatementUtils.splitStatements(text)) {
            runSql(sqlExecutor, stmt);
        }
    }

    private void runSql(SqlExecutor sqlExecutor, String sql) {
        long start = System.currentTimeMillis();
        try {
            CacheTable result = sqlExecutor.executeSql(sql);
            List<RelDataTypeField> fields = result.getDataFields();
            Enumerable<Object[]> enumerable = result.scan(null);
            List<Object[]> rows = enumerable.toList();
            long duration = System.currentTimeMillis() - start;
            List<String> lines = new ArrayList<>(SqlOutputFormatter.format(rows, fields, outputFormat));
            lines.add("Time: " + duration + " ms, " + rows.size() + " row(s)");
            printLines(lines);
        } catch (Exception e) {
            printLines(List.of("exec error: ", ExceptionUtils.getStackTrace(e)));
        }
    }

    /**
     * Print output lines. In interactive mode, use
     * {@link LineReader#printAbove} so that JLine's internal Display
     * tracking stays in sync. In batch mode, print to stdout.
     */
    private void printLines(List<String> lines) {
        if (reader != null) {
            StringBuilder sb = new StringBuilder();
            for (String line : lines) {
                sb.append(line).append('\n');
            }
            reader.printAbove(sb.toString());
        } else {
            lines.forEach(System.out::println);
        }
    }

    // ===================== Interactive REPL =====================

    private int runInteractive(SqlExecutor sqlExecutor) {
        Terminal terminal;
        try {
            terminal = TerminalBuilder.builder().system(true).build();
        } catch (Exception e) {
            System.err.println("Failed to initialize terminal: " + e.getMessage());
            return 1;
        }

        DefaultHistory history = new DefaultHistory();
        this.reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .variable(LineReader.HISTORY_FILE, HISTORY_FILE)
                .variable(LineReader.SECONDARY_PROMPT_PATTERN, PROMPT2)
                .parser(new SqlLineParser())
                .highlighter(new SqlHighlighter())
                .history(history)
                .build();

        try {
            while (true) {
                String line;
                try {
                    line = reader.readLine(PROMPT1);
                } catch (UserInterruptException e) {
                    // Ctrl+C: ignore and show a fresh prompt
                    continue;
                } catch (EndOfFileException e) {
                    // Ctrl+D: exit
                    System.out.println();
                    return 0;
                }
                if (line == null || line.trim().isEmpty()) {
                    continue;
                }
                executeStatements(sqlExecutor, line);
            }
        } finally {
            try {
                history.save();
            } catch (Exception ignored) {
            }
        }
    }
}
