package com.sqlrec.frontend.thrift;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GatewayDisabledTest {
    @Test
    void opensLocalSessionAndRejectsRemoteSqlBeforeReadingEndpointConfiguration() throws Exception {
        String oldAddress = SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.getDefaultValue();
        Integer oldPort = SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.getDefaultValue();
        try {
            SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue("  ");
            SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.setDefaultValue(null);
            assertFalse(SqlRecConfigs.isFlinkSqlGatewayEnabled());
            ClientProxy proxy = new ClientProxy();
            TOpenSessionResp session = proxy.OpenSession(new TOpenSessionReq());
            assertEquals(TStatusCode.SUCCESS_STATUS, session.getStatus().getStatusCode());
            TException failure = assertThrows(TException.class,
                    () -> proxy.ExecuteStatement(new TExecuteStatementReq(session.getSessionHandle(), "SELECT 1")));
            assertTrue(failure.getMessage().startsWith("FLINK_GATEWAY_DISABLED"));
            assertEquals(TStatusCode.SUCCESS_STATUS, proxy.CloseSession(
                    new TCloseSessionReq(session.getSessionHandle())).getStatus().getStatusCode());
        } finally {
            SqlRecConfigs.FLINK_SQL_GATEWAY_ADDRESS.setDefaultValue(oldAddress);
            SqlRecConfigs.FLINK_SQL_GATEWAY_PORT.setDefaultValue(oldPort);
        }
    }
}
