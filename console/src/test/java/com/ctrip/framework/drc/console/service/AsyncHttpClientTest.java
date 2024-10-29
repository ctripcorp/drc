package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.core.http.AsyncHttpClientFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

/**
 * Created by dengquanliang
 * 2024/10/18 10:42
 */
public class AsyncHttpClientTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private AsyncHttpClient asyncHttpClient = AsyncHttpClientFactory.create(1000, 5000, 6000, 1, 100, 1000);
    private static final String url = "http://127.0.0.1:8080/test/cluster/%s";

    @Test
    public void test() throws InterruptedException {
        int n = 101;
        for (int i = 1; i <= n; i++) {
            String cluster = "cluster" + i;
            ListenableFuture<Response> future = asyncHttpClient
                    .preparePut(String.format(url, cluster))
                    .execute();
            future.addListener(() -> {
                try {
                    Response response = future.get();
                    if (response.getStatusCode() != HttpStatus.OK.value()) {
                        logger.error("{}: request fail, {}", cluster, response.getResponseBody());
                    }
                    logger.info("{}: request success", cluster);
                } catch (Throwable e) {
                    logger.error("{}: request error, {}", cluster, e);
                }
            }, MoreExecutors.directExecutor());
        }

        Thread.currentThread().join();

    }
}
