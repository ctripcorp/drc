package com.ctrip.framework.drc.core.utils;

import com.alibaba.dcm.DnsCacheManipulator;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Created by dengquanliang
 * 2025/10/23 15:01
 */
public class DnsCacheUtils {

    private final static Logger logger = LoggerFactory.getLogger(DnsCacheUtils.class);

    public static void setDnsCache(String ip, int port, String expectedResolvedIp) {
        InetSocketAddress address = new InetSocketAddress(ip, port);
        String currentIp = address.getAddress().getHostAddress();
        if (!currentIp.equalsIgnoreCase(expectedResolvedIp)) {
            try {
                DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.dns.cache.set", ip, () -> {
                    DnsCacheManipulator.setDnsCache(ip, expectedResolvedIp);
                    logger.info("[DnsCache] {} setDnsCache from {} to {}", ip, currentIp, expectedResolvedIp);
                });
            } catch (Throwable t) {
                logger.error("[Restart] setDnsCache error for {}", ip, t);
            }
        } else {
            logger.info("[DnsCache] host: {} currentIp: {} equals expectedResolvedIp ", ip, currentIp);
        }
    }
}
