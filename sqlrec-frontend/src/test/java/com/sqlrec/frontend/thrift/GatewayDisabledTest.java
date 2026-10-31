package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class GatewayDisabledTest {
    @Test
    void rejectsRemoteSqlBeforeReadingEndpointConfiguration() throws Exception {
        String configuredAddress = System.getenv("FLINK_SQL_GATEWAY_ADDRESS");
        assumeTrue(configuredAddress == null || configuredAddress.isBlank(),
                "This test exercises disabled defaults; the integration script explicitly enables the Gateway");
        String oldAddress = SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getDefaultValue();
        Integer oldPort = SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.getDefaultValue();
        try {
            SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue("  ");
            SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.setDefaultValue(null);
            assertFalse(SqlRecConfigs.isFlinkSqlGatewayEnabled());
            GatewayClient proxy = new GatewayClient(new TOpenSessionReq());
            TSessionHandle session = new TSessionHandle(com.sqlrec.frontend.utils.ThriftUtils.getHandleIdentifier());
            TException failure = assertThrows(TException.class,
                    () -> proxy.executeStatement(new TExecuteStatementReq(session, "SELECT 1")));
            assertTrue(failure.getMessage().startsWith("FLINK_GATEWAY_DISABLED"));
            assertEquals(TStatusCode.SUCCESS_STATUS, proxy.closeSession().getStatus().getStatusCode());
        } finally {
            SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue(oldAddress);
            SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.setDefaultValue(oldPort);
        }
    }
}
