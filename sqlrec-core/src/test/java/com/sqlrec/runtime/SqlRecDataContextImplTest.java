package com.sqlrec.runtime;

import com.sqlrec.common.config.Consts;
import com.sqlrec.compiler.NormalSqlCompiler;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.runtime.SqlFunctions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlRecDataContextImplTest {
    private final CalciteSchema schema = CalciteSchema.createRootSchema(false);
    private final ExecuteContextImpl executionContext = new ExecuteContextImpl(schema);

    @Test
    void suppliesCalciteTimeZoneAndIdentityVariables() {
        Instant instant = Instant.parse("2026-11-05T20:15:30Z");
        ZoneId zone = ZoneId.of("Asia/Shanghai");
        var context = new SqlRecDataContextImpl(Map.of(), schema, executionContext,
                Clock.fixed(instant, zone));
        long wallTime = instant.toEpochMilli() + 8 * 60 * 60 * 1000L;

        assertEquals(instant.toEpochMilli(), (long) Variable.UTC_TIMESTAMP.get(context));
        assertEquals(wallTime, SqlFunctions.currentTimestamp(context));
        assertEquals(wallTime, SqlFunctions.localTimestamp(context));
        assertEquals(LocalDate.of(2026, 11, 6).toEpochDay(), SqlFunctions.currentDate(context));
        int timeOfDay = (4 * 60 * 60 + 15 * 60 + 30) * 1000;
        assertEquals(timeOfDay, SqlFunctions.currentTime(context));
        assertEquals(timeOfDay, SqlFunctions.localTime(context));
        assertEquals(TimeZone.getTimeZone(zone), SqlFunctions.timeZone(context));
        assertEquals(Locale.ROOT, SqlFunctions.locale(context));
        assertEquals("sa", SqlFunctions.user(context));
        assertEquals(System.getProperty("user.name", ""), SqlFunctions.systemUser(context));
    }

    @Test
    void usesTheZoneOffsetAtTheStatementInstantIncludingDaylightSavingTime() {
        ZoneId zone = ZoneId.of("America/New_York");
        for (String value : new String[]{"2026-01-15T12:00:00Z", "2026-07-15T12:00:00Z"}) {
            Instant instant = Instant.parse(value);
            var context = new SqlRecDataContextImpl(Map.of(), schema, executionContext,
                    Clock.fixed(instant, zone));
            long expected = instant.toEpochMilli() + zone.getRules().getOffset(instant).getTotalSeconds() * 1000L;
            assertEquals(expected, SqlFunctions.localTimestamp(context));
            assertEquals(expected, SqlFunctions.currentTimestamp(context));
        }
    }

    @Test
    void freezesTimeWithinAStatementAndRefreshesItForTheNextStatement() {
        Clock clock = mock(Clock.class);
        when(clock.getZone()).thenReturn(ZoneOffset.UTC);
        when(clock.millis()).thenReturn(1000L, 2000L);
        var first = new SqlRecDataContextImpl(Map.of(), schema, executionContext, clock);
        assertEquals(1000L, SqlFunctions.currentTimestamp(first));
        assertEquals(1000L, SqlFunctions.localTimestamp(first));
        assertEquals(1000L, SqlFunctions.currentTimestamp(first));
        var second = new SqlRecDataContextImpl(Map.of(), schema, executionContext, clock);
        assertEquals(2000L, SqlFunctions.currentTimestamp(second));
        assertEquals(1000L, SqlFunctions.currentTimestamp(first));
    }

    @Test
    void preservesCompiledParametersAndExecutionVariables() {
        Object parameter = new Object();
        executionContext.setVariable("request_id", "test-request");
        var context = new SqlRecDataContextImpl(Map.of("?0", parameter), schema, executionContext);
        assertSame(parameter, context.get("?0"));
        assertNull(context.get("missing"));
        assertEquals("test-request", context.getVariable("request_id"));
        assertSame(executionContext.getVariables(), context.getVariables());
    }

    @Test
    void executesTimeAndUserFunctionsThroughTheSqlCompiler() throws Exception {
        var bindable = NormalSqlCompiler.getNormalSqlBindable(
                "SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP, "
                        + "CURRENT_USER, SESSION_USER, USER, SYSTEM_USER",
                schema, Consts.DEFAULT_SCHEMA_NAME);
        var rows = bindable.bind(schema, executionContext).toList();
        assertEquals(1, rows.size());
        Object[] row = rows.get(0);
        assertEquals(9, row.length);
        for (Object value : row) {
            assertNotNull(value);
        }
        assertEquals(row[1], row[3]);
        assertEquals(row[2], row[4]);
        assertEquals("sa", row[5]);
        assertEquals("sa", row[6]);
        assertEquals("sa", row[7]);
        assertEquals(System.getProperty("user.name", ""), row[8]);
    }
}
