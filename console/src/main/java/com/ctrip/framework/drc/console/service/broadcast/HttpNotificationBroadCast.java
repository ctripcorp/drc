package com.ctrip.framework.drc.console.service.broadcast;

import com.ctrip.framework.drc.console.ha.ConsoleLeaderElector;
import com.ctrip.framework.drc.core.http.AsyncHttpClientFactory;
import com.ctrip.framework.drc.core.server.utils.IpUtils;
import com.google.common.util.concurrent.MoreExecutors;
import java.net.UnknownHostException;
import java.util.List;
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

// notify others the alive nodes in the same region 
@Component
@DependsOn("consoleLeaderElector")
public class HttpNotificationBroadCast implements DisposableBean {
    
    public static final String SCHEMA = "http://%s:8080/%s";
    public static final String BROADCAST = "broadcast";
    public static final String NEED_BROADCAST = "true";
    public static final String NOT_NEED_BROADCAST = "false";
    
    @Autowired
    private ConsoleLeaderElector leaderElector;
    
    private AsyncHttpClient asyncHttpClient = AsyncHttpClientFactory.create(1000,5000,6000,0,100,1000);
    
    private Logger logger = LoggerFactory.getLogger(getClass());
    
    
    public void broadcast(String urlPath, RequestMethod requestMethod, String requestBody) {
        try {
            List<String> othersIp = getOthersIp();
            for (String ip : othersIp) {
                String url = String.format(SCHEMA, ip, urlPath);
                ListenableFuture<Response> future = asyncHttpClient
                        .prepare(requestMethod.name(), url)
                        .addQueryParam(BROADCAST, NOT_NEED_BROADCAST)
                        .setBody(requestBody)
                        .execute();
                logger.debug("broadcasting,url:{},method:{},body:{}", urlPath,requestMethod.name(), requestBody);
                future.addListener(() -> {
                    try {
                        Response response = future.get();
                        if (response.getStatusCode() != HttpStatus.OK.value()) {
                            logger.warn("broadcast not ok,url:{},method:{},body:{},response:{}", urlPath,requestMethod.name(), requestBody,response.getResponseBody());
                        }
                        logger.debug("broadcast success,url:{},method:{},body:{},response",urlPath,requestMethod.name(),requestBody);
                    } catch (Throwable e) {
                        logger.error("broadcast error,url:{},method:{},body:{}",urlPath,requestMethod.name(),requestBody,e);
                    }
                }, MoreExecutors.directExecutor());
            }
        } catch (Throwable e) {
            logger.error("broadcast error,url:{},method:{},body:{}",urlPath,requestMethod.name(),requestBody,e);
        }
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
