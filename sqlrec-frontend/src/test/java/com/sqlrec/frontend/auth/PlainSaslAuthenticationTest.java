package com.sqlrec.frontend.auth;

import com.sqlrec.common.config.SqlRecConfigs;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TSaslServerTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PlainSaslAuthenticationTest {
    private final Boolean previousEnabled = SqlRecConfigs.AUTH_ENABLED.getDefaultValue();
    private final String previousUsername = SqlRecConfigs.AUTH_USERNAME.getDefaultValue();
    private final String previousPassword = SqlRecConfigs.AUTH_PASSWORD.getDefaultValue();

    @BeforeEach
    void configureAuthentication() {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(true);
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue("用户");
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue("secret:密码");
    }

    @AfterEach
    void restoreConfiguration() {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(previousEnabled);
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue(previousUsername);
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue(previousPassword);
    }

    @Test
    void authenticatesUtf8CredentialsAndReusesTheSameTransportForInputAndOutput() throws Exception {
        TSaslServerTransport.Factory factory = PlainSaslAuthentication.transportFactory();
        TMemoryBuffer client = handshake("PLAIN", "\0用户\0secret:密码");
        TSaslServerTransport transport = (TSaslServerTransport) factory.getTransport(client);
        assertTrue(transport.getSaslServer().isComplete());
        assertEquals("用户", transport.getSaslServer().getAuthorizationID());
        assertSame(transport, factory.getTransport(client));
        transport.close();
    }

    @Test
    void rejectsBadPasswordsUsersAndProxyIdentitiesDuringTheHandshake() throws Exception {
        TSaslServerTransport.Factory factory = PlainSaslAuthentication.transportFactory();
        for (String response : List.of("\0用户\0wrong", "\0other\0secret:密码", "\0\0secret:密码",
                "other\0用户\0secret:密码", "malformed", "\0用户\0")) {
            RuntimeException error = assertThrows(RuntimeException.class,
                    () -> factory.getTransport(handshake("PLAIN", response)));
            assertFalse(error.toString().contains("secret:密码"));
        }
    }

    @Test
    void refusesOtherSaslMechanisms() throws Exception {
        TSaslServerTransport.Factory factory = PlainSaslAuthentication.transportFactory();
        assertThrows(RuntimeException.class, () -> factory.getTransport(handshake("ANONYMOUS", "")));
    }

    private static TMemoryBuffer handshake(String mechanism, String response) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream frames = new DataOutputStream(bytes);
        byte[] name = mechanism.getBytes(StandardCharsets.UTF_8);
        byte[] credentials = response.getBytes(StandardCharsets.UTF_8);
        frames.writeByte(1); // Thrift SASL START
        frames.writeInt(name.length);
        frames.write(name);
        frames.writeByte(2); // Thrift SASL OK, containing the PLAIN initial response
        frames.writeInt(credentials.length);
        frames.write(credentials);
        TMemoryBuffer transport = new TMemoryBuffer(bytes.size() + 128);
        transport.write(bytes.toByteArray());
        return transport;
    }
}
