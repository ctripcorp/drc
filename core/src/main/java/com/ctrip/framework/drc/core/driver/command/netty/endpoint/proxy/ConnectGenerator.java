package com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy.ProxyProtocol.KEY_WORD;
import static com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy.ProxyProtocol.ROUTE;

/**
 * @Author limingdong
 * @create 2021/4/9
 */
public class ConnectGenerator {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String ip;

    private int port;

    private String originRouteInfo;

    public ConnectGenerator(String ip, int port, String routeInfo) {
        this.ip = ip;
        this.port = port;
        if (StringUtils.isNotBlank(routeInfo) && routeInfo.trim().startsWith(KEY_WORD)) {
            originRouteInfo = routeInfo.trim();
            originRouteInfo = originRouteInfo.substring(KEY_WORD.length(), originRouteInfo.length()).trim();
            if (originRouteInfo.startsWith(ROUTE)) {
                originRouteInfo = originRouteInfo.substring(ROUTE.length(), originRouteInfo.length()).trim();
            }
        } else {
            originRouteInfo = routeInfo;
        }
    }

    public Endpoint generate() {
        if(StringUtils.isNotBlank(originRouteInfo)) {
            List<Endpoint> candidates = getConnectEndpoints();
            if (!candidates.isEmpty()) {
                Endpoint candidate = candidates.get(0);
                return new ProxyEnabledEndpoint(candidate.getHost(), candidate.getPort(), getProxyProtocol());
            }
            throw new IllegalArgumentException("no proxy");
        } else {
            return new DefaultEndPoint(ip, port);
        }
    }

    protected ProxyProtocol getProxyProtocol() {
        String lastHop = String.format("://%s:%s", ip, port);
        int firstIndex = originRouteInfo.indexOf(" ");
        String protocol = lastHop;
        if (firstIndex >= 0) {
            String restProxy = originRouteInfo.substring(firstIndex + 1, originRouteInfo.length());
            protocol = String.format("%s%s", restProxy, lastHop);
        }
        logger.info("[getProxyProtocol] protocol: {}", protocol);
        return new DrcProxyProtocol(protocol);
    }

    private List<Endpoint> getConnectEndpoints() {
        List<Endpoint> res = Lists.newArrayList();

        String[] endpoints = originRouteInfo.split(" ");
        if (endpoints != null && endpoints.length > 0) {
            String firstHop = endpoints[0];
            String[] firstProxy = firstHop.split(",");
            for (String proxy : firstProxy) {
                int index = proxy.lastIndexOf("/");
                if (index >= 0) {
                    String ipAndPort = proxy.substring(index + 1, proxy.length());
                    Endpoint endpoint = parseEndpoint(ipAndPort);
                    if (endpoint != null) {
                        res.add(endpoint);
                    }
                }
            }
        }
        return res;
    }

    private Endpoint parseEndpoint(String ipAndPort) {
        if (StringUtils.isNotBlank(ipAndPort)) {
            String[] split = ipAndPort.split(":");
            if (split.length == 2) {
                return new DefaultEndPoint(split[0], Integer.parseInt(split[1]));
            }
        }
        return null;
    }
}
