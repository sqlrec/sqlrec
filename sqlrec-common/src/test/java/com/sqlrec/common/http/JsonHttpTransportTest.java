package com.sqlrec.common.http;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JsonHttpTransportTest {
    @Mock private OkHttpClient client;
    @Mock private Call call;
    @Mock private Response response;
    @Mock private ResponseBody body;

    @BeforeEach
    void setUp() throws IOException {
        when(client.newCall(any(Request.class))).thenReturn(call);
        when(call.execute()).thenReturn(response);
    }

    @Test
    void returnsRawBodyWithoutProtocolParsingAndClosesTheResponse() throws IOException {
        when(response.isSuccessful()).thenReturn(true);
        when(response.body()).thenReturn(body);
        when(body.string()).thenReturn("not JSON");

        assertEquals("not JSON", JsonHttpTransport.post(client, "http://test", "{}"));
        verify(response).close();
    }

    @Test
    void closesErrorResponseWithoutReadingItsBody() {
        when(response.isSuccessful()).thenReturn(false);
        when(response.code()).thenReturn(503);

        RuntimeException failure = assertThrows(RuntimeException.class,
                () -> JsonHttpTransport.post(client, "http://test", "{}"));
        assertEquals("HTTP request failed with response code: 503", failure.getMessage());
        verify(response).close();
        verify(response, never()).body();
    }

    @Test
    void preservesReadFailureAndStillClosesTheResponse() throws IOException {
        IOException failure = new IOException("read failed");
        when(response.isSuccessful()).thenReturn(true);
        when(response.body()).thenReturn(body);
        when(body.string()).thenThrow(failure);

        assertSame(failure, assertThrows(IOException.class,
                () -> JsonHttpTransport.post(client, "http://test", "{}")));
        verify(response).close();
    }
}
