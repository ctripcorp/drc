package com.ctrip.framework.drc.monitor.controller;

import com.ctrip.framework.drc.monitor.DrcMonitorModule;
import com.ctrip.framework.drc.monitor.MonitorStarter;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by jixinwang on 2022/11/23
 */
@Component
public class StartCase {

    private static Logger logger = LoggerFactory.getLogger(MonitorStarter.class);

    @PostConstruct
    private void start() {
        String srcIp = ConfigService.getInstance().getDrcMonitorMysqlSrcIp();
        logger.info("srcIp is {}", srcIp);
        String srcPort = ConfigService.getInstance().getDrcMonitorMysqlSrcPort();
        logger.info("srcPort is {}", srcPort);

        String dstIp = ConfigService.getInstance().getDrcMonitorMysqlDstIp();
        logger.info("dstIp is {}", dstIp);
        String dstPort = ConfigService.getInstance().getDrcMonitorMysqlDstPort();
        logger.info("dstPort is {}", dstPort);

        String user = ConfigService.getInstance().getDrcMonitorMysqlUser();
        logger.info("user is {}", user);
        String password = ConfigService.getInstance().getDrcMonitorMysqlPassword();
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
