package com.sqlrec.frontend.auth;

import org.apache.hadoop.security.SaslPlainServer;
import org.apache.thrift.transport.TSaslServerTransport;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import java.security.Security;
import java.util.Map;

/** Standard Thrift SASL PLAIN handshake, using Hadoop's existing PLAIN server provider. */
public final class PlainSaslAuthentication {
    private PlainSaslAuthentication() {
    }

    public static TSaslServerTransport.Factory transportFactory() {
        // The JDK supplies a PLAIN client; Hadoop supplies the server implementation.
        Security.addProvider(new SaslPlainServer.SecurityProvider());
        return new TSaslServerTransport.Factory("PLAIN", null, null,
                Map.of(Sasl.QOP, "auth", Sasl.POLICY_NOPLAINTEXT, "false"), callbacks -> {
                    String username = null;
                    PasswordCallback password = null;
                    AuthorizeCallback authorization = null;
                    for (Callback callback : callbacks) {
                        if (callback instanceof NameCallback name) {
                            username = name.getName();
                        } else if (callback instanceof PasswordCallback secret) {
                            password = secret;
                        } else if (callback instanceof AuthorizeCallback authorize) {
                            authorization = authorize;
                        } else {
                            throw new UnsupportedCallbackException(callback);
                        }
                    }
                    try {
                        if (username == null || password == null || password.getPassword() == null
                                || authorization == null
                                || !username.equals(authorization.getAuthorizationID())
                                || !Authentication.authenticate(username, new String(password.getPassword()))) {
                            throw new AuthenticationException("authentication failed");
                        }
                        // Only the authenticated identity is accepted; no proxy identities.
                        authorization.setAuthorized(true);
                    } finally {
                        if (password != null) {
                            password.clearPassword();
                        }
                    }
                });
    }
}
