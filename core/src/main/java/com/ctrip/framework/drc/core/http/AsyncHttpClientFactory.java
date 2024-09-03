package com.ctrip.framework.drc.core.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;

/**
 * @ClassName AsyncHttpClientFactory
 * @Author haodongPan
 * @Date 2024/7/31 10:55
 * @Version: $
 */
public class AsyncHttpClientFactory {
    
    public static final List<AsyncHttpClient> clients = new ArrayList<>();

    // must close before application exit
    public static AsyncHttpClient create(int connectTimeOut,int readTimeOut,int requestTimeOut, int maxRetry,int maxConnectionsPerHost,int maxConnections) {
        AsyncHttpClient asyncHttpClient = Dsl.asyncHttpClient(Dsl.config()
                .setConnectTimeout(connectTimeOut)
                .setReadTimeout(readTimeOut)
                .setRequestTimeout(requestTimeOut)
                .setMaxRequestRetry(maxRetry)
                .setMaxConnectionsPerHost(maxConnectionsPerHost)
                .setMaxConnections(maxConnections)
                .setDisableHttpsEndpointIdentificationAlgorithm(true)
                .setUseInsecureTrustManager(true)
        );
        clients.add(asyncHttpClient);
        return asyncHttpClient;
    }
    
    public static void closeAll() throws IOException {
        for (AsyncHttpClient client : clients) {
            client.close();
        }
    }

}
