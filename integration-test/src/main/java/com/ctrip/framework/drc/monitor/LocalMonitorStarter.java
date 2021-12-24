package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.core.server.config.SystemConfig;

/**
 * Created by mingdongli
 * 2019/11/15 上午10:06.
 */
public class LocalMonitorStarter  extends AbstractStarter {

    public static void main(String[] args) {

        System.setProperty(SystemConfig.BENCHMARK_SWITCH_TEST, String.valueOf(true));

        String srcIp = getPropertyOrDefault("drc.monitor.mysql.src.ip", "127.0.0.1");
        logger.info("srcIp is {}", srcIp);
        String srcPort = getPropertyOrDefault("drc.monitor.mysql.src.port", "3306");
        logger.info("srcPort is {}", srcPort);

        String dstIp = getPropertyOrDefault("drc.monitor.mysql.dst.ip", "127.0.0.1");
        logger.info("dstIp is {}", dstIp);
        String dstPort = getPropertyOrDefault("drc.monitor.mysql.dst.port", "3307");
        logger.info("dstPort is {}", dstPort);

        String user = getPropertyOrDefault("drc.monitor.mysql.user", "root");
        logger.info("user is {}", user);
        String password = getPropertyOrDefault("drc.monitor.mysql.password", "root");
        logger.info("password is {}", password);

        try {
            DrcMonitorModule drcMonitorModule = new DrcMonitorModule(srcIp, Integer.parseInt(srcPort), dstIp, Integer.parseInt(dstPort), user, password);
            drcMonitorModule.initialize();
            drcMonitorModule.start();
            Thread.currentThread().join();
        } catch (Exception e) {
            logger.error("ReplicatorStarter error", e);
        }
    }
}

