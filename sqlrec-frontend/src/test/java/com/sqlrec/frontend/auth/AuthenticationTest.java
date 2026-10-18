package com.sqlrec.frontend.auth;

import com.sqlrec.common.config.SqlRecConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AuthenticationTest {
    private final Boolean previousEnabled = SqlRecConfigs.AUTH_ENABLED.getDefaultValue();
    private final String previousUsername = SqlRecConfigs.AUTH_USERNAME.getDefaultValue();
    private final String previousPassword = SqlRecConfigs.AUTH_PASSWORD.getDefaultValue();

    @BeforeEach
    void configureAuthentication() {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(true);
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue("sqlrec");
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue("secret:with:colons");
    }

    @AfterEach
    void restoreConfiguration() {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(previousEnabled);
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue(previousUsername);
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue(previousPassword);
    }

    @Test
    void rejectsMissingMalformedAndIncorrectCredentialsWithoutThrowing() {
        assertFalse(Authentication.authenticateBasic(null));
        for (String header : List.of("", "Basic", "Bearer token", "Basic !!!", "Basic ",
                basic("sqlrec:wrong"), basic("wrong:secret:with:colons"), basic("sqlrec"))) {
            assertFalse(Authentication.authenticateBasic(header), header);
        }
        assertFalse(Authentication.authenticate(null, null));
        assertFalse(Authentication.authenticate("sqlrec", "wrong"));
        assertFalse(Authentication.authenticate("wrong", "secret:with:colons"));
        assertTrue(Authentication.authenticateBasic(basic("sqlrec:secret:with:colons")));
        assertTrue(Authentication.authenticate("sqlrec", "secret:with:colons"));
    }

    @Test
    void supportsUtf8AndCaseInsensitiveSchemeButCaseSensitiveCredentials() {
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue("用户");
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue("密码: Secret ");
        assertDoesNotThrow(Authentication::validateConfig);
        assertTrue(Authentication.authenticateBasic(basic("用户:密码: Secret ").replace("Basic", "bAsIc")));
        assertTrue(Authentication.authenticate("用户", "密码: Secret "));
        assertFalse(Authentication.authenticate("用户", "密码: secret "));
    }

    @Test
    void disabledAuthenticationKeepsAnonymousAccess() {
        SqlRecConfigs.AUTH_ENABLED.setDefaultValue(false);
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue("");
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue("");
        assertFalse(Authentication.isEnabled());
        assertDoesNotThrow(Authentication::validateConfig);
        assertTrue(Authentication.authenticateBasic(null));
        assertTrue(Authentication.authenticate(null, null));
    }

    @Test
    void enabledAuthenticationRequiresValidConfigurationWithoutEchoingSecrets() {
        for (String username : List.of("", " ", "user:name", "user\nname")) {
            SqlRecConfigs.AUTH_USERNAME.setDefaultValue(username);
            assertThrows(IllegalArgumentException.class, Authentication::validateConfig);
        }
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue("sqlrec");
        for (String password : List.of("", " ", "secret\nvalue")) {
            SqlRecConfigs.AUTH_PASSWORD.setDefaultValue(password);
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    Authentication::validateConfig);
            assertEquals("AUTH_PASSWORD must be non-blank and contain no control characters", error.getMessage());
        }
    }

    @Test
    void readsProcessConfigurationWithoutACache() {
        assertTrue(Authentication.isEnabled());
        assertTrue(Authentication.authenticate("sqlrec", "secret:with:colons"));
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue("changed");
        assertFalse(Authentication.authenticate("sqlrec", "secret:with:colons"));
        assertTrue(Authentication.authenticate("sqlrec", "changed"));
        assertTrue(Authentication.authenticateBasic(basic("sqlrec:changed")));
    }

    private static String basic(String credentials) {
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }
}
