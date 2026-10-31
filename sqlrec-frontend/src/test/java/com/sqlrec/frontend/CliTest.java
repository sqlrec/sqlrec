package com.sqlrec.frontend;

import com.sqlrec.common.schema.CacheTable;
import com.sqlrec.common.utils.DataTypeUtils;
import com.sqlrec.executor.SqlExecutor;
import org.apache.calcite.linq4j.Linq4j;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CliTest {
    @Test
    void inlineBatchReturnsNonzeroAndStopsAfterFirstFailure() throws Exception {
        assertBatchFailure("-e", "select 1; select 2;");
    }

    @Test
    void fileBatchReturnsNonzeroAndStopsAfterFirstFailure(@TempDir Path directory) throws Exception {
        Path sql = directory.resolve("batch.sql");
        Files.writeString(sql, "select 1; select 2;");
        assertBatchFailure("-f", sql.toString());
    }

    @Test
    void successfulBatchReturnsZeroAndExecutesEveryStatement() throws Exception {
        try (MockedConstruction<SqlExecutor> executors = mockConstruction(SqlExecutor.class,
                (executor, context) -> when(executor.executeSql(anyString())).thenReturn(new CacheTable(
                        "result", Linq4j.singletonEnumerable(new Object[]{"done"}),
                        DataTypeUtils.getStringTypeField("value"))))) {
            assertEquals(0, new CommandLine(new Cli()).execute("-e", "select 1; select 2;"));
            var order = inOrder(executors.constructed().getFirst());
            order.verify(executors.constructed().getFirst()).executeSql("select 1");
            order.verify(executors.constructed().getFirst()).executeSql("select 2");
        }
    }

    private static void assertBatchFailure(String... arguments) throws Exception {
        PrintStream originalOutput = System.out;
        PrintStream originalError = System.err;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        try (PrintStream capturedOutput = new PrintStream(output, true, StandardCharsets.UTF_8);
             PrintStream capturedError = new PrintStream(error, true, StandardCharsets.UTF_8);
             MockedConstruction<SqlExecutor> executors = mockConstruction(SqlExecutor.class,
                     (executor, context) -> when(executor.executeSql("select 1"))
                             .thenThrow(new IllegalArgumentException("batch SQL failed")))) {
            System.setOut(capturedOutput);
            System.setErr(capturedError);
            assertEquals(1, new CommandLine(new Cli()).execute(arguments));
            SqlExecutor executor = executors.constructed().getFirst();
            verify(executor).executeSql("select 1");
            verify(executor, never()).executeSql("select 2");
            assertTrue(error.toString(StandardCharsets.UTF_8).contains("batch SQL failed"));
            assertEquals("", output.toString(StandardCharsets.UTF_8));
        } finally {
            System.setOut(originalOutput);
            System.setErr(originalError);
        }
    }

    @Test
    void preservesCommentsAsTokenSeparatorsAndQuotedSemicolonsInInteractiveInput() throws Throwable {
        try (InteractiveFixture fixture = new InteractiveFixture()) {
            when(fixture.reader.readLine("sqlrec> "))
                    .thenReturn("SELECT/**/1 AS `a;b`;")
                    .thenThrow(new EndOfFileException());
            assertEquals(0, runInteractive(new Cli(), fixture.executor));
            verify(fixture.executor).executeSql("SELECT 1 AS `a;b`");
            verifyNoMoreInteractions(fixture.executor);
        }
    }

    @Test
    void ignoresInterruptAndBlankInputContinuesAfterSqlFailureAndSavesHistoryOnEof() throws Throwable {
        try (InteractiveFixture fixture = new InteractiveFixture()) {
            when(fixture.reader.readLine("sqlrec> "))
                    .thenThrow(new UserInterruptException("unfinished"))
                    .thenReturn((String) null).thenReturn(" \t", "select 1; select 2;")
                    .thenThrow(new EndOfFileException());
            IllegalArgumentException failure = new IllegalArgumentException("first statement failed");
            when(fixture.executor.executeSql("select 1")).thenThrow(failure);
            when(fixture.executor.executeSql("select 2")).thenReturn(new CacheTable("result",
                    Linq4j.asEnumerable(Collections.singletonList(new Object[]{"done"})),
                    DataTypeUtils.getStringTypeField("value")));

            assertEquals(0, runInteractive(new Cli(), fixture.executor));

            var order = inOrder(fixture.executor);
            order.verify(fixture.executor).executeSql("select 1");
            order.verify(fixture.executor).executeSql("select 2");
            verify(fixture.reader).printAbove(argThat((String text) ->
                    text.startsWith("exec error: \n") && text.contains("first statement failed")));
            verify(fixture.reader).printAbove(argThat((String text) ->
                    text.contains("done") && text.endsWith(" row(s)\n")));
            verify(fixture.history()).save();
            assertEquals(System.lineSeparator(), fixture.output.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    void savesHistoryWhenReadingFailsWithoutReplacingTheOriginalFailure() throws Throwable {
        try (InteractiveFixture fixture = new InteractiveFixture()) {
            IllegalStateException failure = new IllegalStateException("reader failed");
            when(fixture.reader.readLine("sqlrec> ")).thenThrow(failure);
            // Reader construction creates the history; its save failure must remain suppressed.
            fixture.readerBuilders.when(LineReaderBuilder::builder).thenAnswer(call -> {
                doThrow(new IOException("history failed")).when(fixture.history()).save();
                return fixture.readerBuilder;
            });

            assertSame(failure, assertThrows(IllegalStateException.class,
                    () -> runInteractive(new Cli(), fixture.executor)));
            verify(fixture.history()).save();
            verifyNoInteractions(fixture.executor);
        }
    }

    @Test
    void historySaveFailureDoesNotChangeSuccessfulEofExit() throws Throwable {
        try (InteractiveFixture fixture = new InteractiveFixture()) {
            when(fixture.reader.readLine("sqlrec> ")).thenThrow(new EndOfFileException());
            fixture.readerBuilders.when(LineReaderBuilder::builder).thenAnswer(call -> {
                doThrow(new IOException("history failed")).when(fixture.history()).save();
                return fixture.readerBuilder;
            });

            assertEquals(0, runInteractive(new Cli(), fixture.executor));
            verify(fixture.history()).save();
            verifyNoInteractions(fixture.executor);
        }
    }

    @Test
    void terminalInitializationFailureReturnsOneWithoutCreatingReaderOrHistory() throws Throwable {
        PrintStream originalError = System.err;
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        try (InteractiveFixture fixture = new InteractiveFixture();
             PrintStream capturedError = new PrintStream(error, true, StandardCharsets.UTF_8)) {
            System.setErr(capturedError);
            when(fixture.terminalBuilder.build()).thenThrow(new IOException("terminal unavailable"));

            assertEquals(1, runInteractive(new Cli(), fixture.executor));
            assertEquals("Failed to initialize terminal: terminal unavailable" + System.lineSeparator(),
                    error.toString(StandardCharsets.UTF_8));
            assertTrue(fixture.histories.constructed().isEmpty());
            fixture.readerBuilders.verifyNoInteractions();
        } finally {
            System.setErr(originalError);
        }
    }

    private static int runInteractive(Cli cli, SqlExecutor executor) throws Throwable {
        var method = Cli.class.getDeclaredMethod("runInteractive", SqlExecutor.class);
        method.setAccessible(true);
        try {
            return (int) method.invoke(cli, executor);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static final class InteractiveFixture implements AutoCloseable {
        private final SqlExecutor executor = mock(SqlExecutor.class);
        private final LineReader reader = mock(LineReader.class);
        private final TerminalBuilder terminalBuilder = mock(TerminalBuilder.class, RETURNS_SELF);
        private final LineReaderBuilder readerBuilder = mock(LineReaderBuilder.class, RETURNS_SELF);
        private final MockedStatic<TerminalBuilder> terminalBuilders = mockStatic(TerminalBuilder.class);
        private final MockedStatic<LineReaderBuilder> readerBuilders = mockStatic(LineReaderBuilder.class);
        private final MockedConstruction<DefaultHistory> histories = mockConstruction(DefaultHistory.class);
        private final PrintStream originalOutput = System.out;
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private final PrintStream capturedOutput = new PrintStream(output, true, StandardCharsets.UTF_8);

        private InteractiveFixture() throws IOException {
            terminalBuilders.when(TerminalBuilder::builder).thenReturn(terminalBuilder);
            when(terminalBuilder.build()).thenReturn(mock(Terminal.class));
            readerBuilders.when(LineReaderBuilder::builder).thenReturn(readerBuilder);
            when(readerBuilder.build()).thenReturn(reader);
            System.setOut(capturedOutput);
        }

        private DefaultHistory history() {
            return histories.constructed().get(0);
        }

        @Override
        public void close() {
            System.setOut(originalOutput);
            capturedOutput.close();
            histories.close();
            readerBuilders.close();
            terminalBuilders.close();
        }
    }
}
