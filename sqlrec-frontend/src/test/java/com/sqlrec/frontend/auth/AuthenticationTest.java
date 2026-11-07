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
    void authenticatesOnlyMatchingPairsAndPreservesWhitespace() {
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue("sqlrec,用户, alice ");
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue("secret:with:colons,密码: Secret , spaced ");
        assertDoesNotThrow(Authentication::validateConfig);
        for (String[] account : List.of(new String[]{"sqlrec", "secret:with:colons"},
                new String[]{"用户", "密码: Secret "}, new String[]{" alice ", " spaced "})) {
            assertTrue(Authentication.authenticate(account[0], account[1]));
            assertTrue(Authentication.authenticateBasic(basic(account[0] + ":" + account[1])));
        }
        for (String[] account : List.of(new String[]{"sqlrec", "密码: Secret "},
                new String[]{"用户", "secret:with:colons"}, new String[]{"alice", " spaced "},
                new String[]{" alice ", "spaced"}, new String[]{"SQLREC", "secret:with:colons"},
                new String[]{"sqlrec,用户, alice ", "secret:with:colons,密码: Secret , spaced "})) {
            assertFalse(Authentication.authenticate(account[0], account[1]));
            assertFalse(Authentication.authenticateBasic(basic(account[0] + ":" + account[1])));
        }
    }

    @Test
    void rejectsMismatchedCountsEmptyEntriesAndDuplicateUsernamesWithoutEchoingSecrets() {
        for (String[] config : List.of(new String[]{"alice,bob", "secret"},
                new String[]{"alice", "secret,second"}, new String[]{"alice,", "secret,second"},
                new String[]{",bob", "secret,second"}, new String[]{"alice,,bob", "secret,middle,second"},
                new String[]{"alice,bob", "secret,"}, new String[]{"alice,bob", ",second"},
                new String[]{"alice,bob,carol", "secret,,third"}, new String[]{"alice, ", "secret,second"},
                new String[]{"alice,bob", "secret, "}, new String[]{"alice,b:ob", "secret,second"},
                new String[]{"alice,bo\nb", "secret,second"}, new String[]{"alice,bob", "secret,sec\nond"},
                new String[]{"alice,alice", "secret,second"})) {
            SqlRecConfigs.AUTH_USERNAME.setDefaultValue(config[0]);
            SqlRecConfigs.AUTH_PASSWORD.setDefaultValue(config[1]);
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, Authentication::validateConfig);
            assertFalse(error.getMessage().contains("secret"));
            assertFalse(error.getMessage().contains("second"));
        }
    }

    @Test
    void distinctUsernamesMayShareAPassword() {
        SqlRecConfigs.AUTH_USERNAME.setDefaultValue("alice,Alice");
        SqlRecConfigs.AUTH_PASSWORD.setDefaultValue("secret,secret");
        assertDoesNotThrow(Authentication::validateConfig);
        assertTrue(Authentication.authenticate("alice", "secret"));
        assertTrue(Authentication.authenticate("Alice", "secret"));
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
