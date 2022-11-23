package com.ctrip.framework.drc.monitor;

import com.ctrip.framework.drc.monitor.config.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.support.SpringBootServletInitializer;

/**
 * Created by mingdongli
 * 2019/11/1 下午4:56.
 */
@ServletComponentScan
@SpringBootApplication
public class MonitorStarter extends SpringBootServletInitializer {

    private static Logger logger = LoggerFactory.getLogger(MonitorStarter.class);

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(MonitorStarter.class);
    }

    public static void main(String[] args) {

        new SpringApplicationBuilder(MonitorStarter.class).run(args);

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
