package com.ctrip.framework.drc.console.service.broadcast;

import com.ctrip.framework.drc.console.ha.ConsoleLeaderElector;
import com.ctrip.framework.drc.core.http.AsyncHttpClientFactory;
import com.ctrip.framework.drc.core.server.utils.IpUtils;
import com.google.common.util.concurrent.MoreExecutors;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMethod;

import java.net.UnknownHostException;
import java.util.List;

// notify others the alive nodes in the same region 
@Component
@DependsOn("consoleLeaderElector")
public class HttpNotificationBroadCast implements DisposableBean {

    public static final String SCHEMA = "http://%s:8080/%s";

    @Autowired
    private ConsoleLeaderElector leaderElector;

    private AsyncHttpClient asyncHttpClient = AsyncHttpClientFactory.create(1000, 5000, 6000, 0, 100, 1000);

    private Logger logger = LoggerFactory.getLogger(getClass());


    public void broadcastWithRetry(String urlPath, RequestMethod requestMethod, String requestBody, int retryTime) {
        try {
            List<String> othersIp = getOthersIp();
            for (String ip : othersIp) {
                String url = String.format(SCHEMA, ip, urlPath);
                broadcast(url, requestMethod, requestBody, retryTime);
            }
        } catch (Throwable e) {
            logger.error("broadcast error,url:{},method:{},body:{}", urlPath, requestMethod.name(), requestBody, e);
        }
    }

    private void broadcast(String url, RequestMethod requestMethod, String requestBody, int retryTime) {
        ListenableFuture<Response> future = asyncHttpClient
                .prepare(requestMethod.name(), url)
                .setHeader("Accept", "application/json")
                .setHeader("Content-Type", "application/json; charset=utf-8")
                .setBody(requestBody)
                .execute();
        int nextRetryTime = retryTime - 1;
        future.addListener(() -> {
            try {
                Response response = future.get();
                if (response.getStatusCode() != HttpStatus.OK.value()) {
                    logger.error("broadcast fail,retryTime: {}, url: {},method: {},body: {},response: {}", retryTime, url, requestMethod.name(), requestBody, response.getResponseBody());
                    if (retryTime > 0) {
                        broadcast(url, requestMethod, requestBody, nextRetryTime);
                    }
                }
                logger.info("broadcast success,url:{},method:{},body:{}", url, requestMethod.name(), requestBody);
            } catch (Throwable e) {
                logger.error("broadcast error,retryTime: {}, url: {},method: {},body: {}",retryTime, url, requestMethod.name(), requestBody, e);
                if (retryTime > 0) {
                    broadcast(url, requestMethod, requestBody, nextRetryTime);
                }
            }
        }, MoreExecutors.directExecutor());
    }

    public List<String> getOthersIp() throws UnknownHostException {
        List<String> allServers = leaderElector.getAllServers();
        String hostAddress = IpUtils.getFistNonLocalIpv4ServerAddress();
        allServers.remove(hostAddress);
        return allServers;
    }

    @Override
    public void destroy() throws Exception {
        asyncHttpClient.close();
    }
}
