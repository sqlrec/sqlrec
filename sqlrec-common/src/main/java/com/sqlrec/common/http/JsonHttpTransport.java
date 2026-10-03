package com.sqlrec.common.http;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;

/** JSON POST transport; clients retain their own serialization and timeout policies. */
public final class JsonHttpTransport {
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private JsonHttpTransport() {
    }

    public static String post(OkHttpClient client, String url, String bodyJson) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(bodyJson, JSON))
                .addHeader("Accept", "application/json")
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new RuntimeException("HTTP request failed with response code: " + response.code());
            }
            return response.body() != null ? response.body().string() : "";
        }
    }
}
