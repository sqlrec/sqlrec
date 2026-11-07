package com.sqlrec.frontend.auth;

import com.sqlrec.common.config.SqlRecConfigs;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

/** Process-wide accounts shared by HTTP Basic and Thrift SASL PLAIN. */
public final class Authentication {
    private Authentication() {
    }

    /** Validates process configuration before a server accesses metadata or binds a port. */
    public static void validateConfig() {
        if (!isEnabled()) {
            return;
        }
        String[] usernames = SqlRecConfigs.AUTH_USERNAME.getValue().split(",", -1);
        String[] passwords = SqlRecConfigs.AUTH_PASSWORD.getValue().split(",", -1);
        if (usernames.length != passwords.length) {
            throw new IllegalArgumentException("AUTH_USERNAME and AUTH_PASSWORD must contain the same number of entries");
        }
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < usernames.length; i++) {
            String username = usernames[i];
            String password = passwords[i];
            if (username.isBlank() || username.contains(":") || username.chars().anyMatch(Character::isISOControl)) {
                throw new IllegalArgumentException("AUTH_USERNAME must be non-blank and contain no colon or control characters");
            }
            if (!seen.add(username)) {
                throw new IllegalArgumentException("AUTH_USERNAME must not contain duplicate usernames");
            }
            if (password.isBlank() || password.chars().anyMatch(Character::isISOControl)) {
                throw new IllegalArgumentException("AUTH_PASSWORD must be non-blank and contain no control characters");
            }
        }
    }

    public static boolean isEnabled() {
        return SqlRecConfigs.AUTH_ENABLED.getValue();
    }

    public static boolean authenticate(String username, String password) {
        if (!isEnabled()) {
            return true;
        }
        if (username == null || password == null) {
            return false;
        }
        String[] usernames = SqlRecConfigs.AUTH_USERNAME.getValue().split(",", -1);
        String[] passwords = SqlRecConfigs.AUTH_PASSWORD.getValue().split(",", -1);
        if (usernames.length != passwords.length) {
            return false;
        }
        byte[] suppliedUsername = bytes(username);
        byte[] suppliedPassword = bytes(password);
        boolean authenticated = false;
        for (int i = 0; i < usernames.length; i++) {
            authenticated |= MessageDigest.isEqual(bytes(usernames[i]), suppliedUsername)
                    & MessageDigest.isEqual(bytes(passwords[i]), suppliedPassword);
        }
        return authenticated;
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
            String[] usernames = SqlRecConfigs.AUTH_USERNAME.getValue().split(",", -1);
            String[] passwords = SqlRecConfigs.AUTH_PASSWORD.getValue().split(",", -1);
            if (usernames.length != passwords.length) {
                return false;
            }
            boolean authenticated = false;
            for (int i = 0; i < usernames.length; i++) {
                authenticated |= MessageDigest.isEqual(bytes(usernames[i] + ":" + passwords[i]), credentials);
            }
            return authenticated;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
