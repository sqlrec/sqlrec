package com.sqlrec.frontend.auth;

import com.sqlrec.common.config.SqlRecConfigs;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/** One process-wide account shared by HTTP Basic and Thrift SASL PLAIN. */
public final class Authentication {
    private Authentication() {
    }

    /** Validates process configuration before a server accesses metadata or binds a port. */
    public static void validateConfig() {
        if (!isEnabled()) {
            return;
        }
        String username = SqlRecConfigs.AUTH_USERNAME.getValue();
        String password = SqlRecConfigs.AUTH_PASSWORD.getValue();
        if (username.isBlank() || username.contains(":") || username.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("AUTH_USERNAME must be non-blank and contain no colon or control characters");
        }
        if (password.isBlank() || password.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("AUTH_PASSWORD must be non-blank and contain no control characters");
        }
    }

    public static boolean isEnabled() {
        return SqlRecConfigs.AUTH_ENABLED.getValue();
    }

    public static boolean authenticate(String username, String password) {
        if (!isEnabled()) {
            return true;
        }
        return username != null && password != null
                && (MessageDigest.isEqual(bytes(SqlRecConfigs.AUTH_USERNAME.getValue()), bytes(username))
                & MessageDigest.isEqual(bytes(SqlRecConfigs.AUTH_PASSWORD.getValue()), bytes(password)));
    }

    public static boolean authenticateBasic(String authorization) {
        if (!isEnabled()) {
            return true;
        }
        if (authorization == null) {
            return false;
        }
        int separator = authorization.indexOf(' ');
        if (separator < 0 || !authorization.substring(0, separator).equalsIgnoreCase("Basic")) {
            return false;
        }
        try {
            byte[] credentials = Base64.getDecoder().decode(authorization.substring(separator + 1).trim());
            byte[] expected = bytes(SqlRecConfigs.AUTH_USERNAME.getValue() + ":" + SqlRecConfigs.AUTH_PASSWORD.getValue());
            return MessageDigest.isEqual(expected, credentials);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
