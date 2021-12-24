package com.ctrip.framework.drc.core.monitor.kpi;

import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.enums.DirectionEnum;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2020/3/17
 */
public class DelayMonitorReportTest {

    private DelayMonitorReport delayMonitorReport;

    private TrafficEntity trafficEntity;

    private long domain = 12345L;

    @Before
    public void setUp() throws Exception {
        String ip = "127.0.0.1";
        if (StringUtils.isBlank(ip)) {
            ip = SystemConfig.LOCAL_SERVER_ADDRESS;
        }
        trafficEntity = new TrafficEntity.Builder()
                .clusterAppId(domain)
                .buName("ABC")
                .dcName("SHAOY")
                .clusterName("clusterName")
                .ip(ip)
                .port(1234)
                .direction(DirectionEnum.OUT.getDescription())
                .module(ModuleEnum.REPLICATOR.getDescription())
                .build();

        delayMonitorReport = new DelayMonitorReport(domain, trafficEntity);
        delayMonitorReport.initialize();
        delayMonitorReport.start();
    }

    @Test
    public void sendGtid() throws InterruptedException {
        String gtid = "1234:1";
        delayMonitorReport.receiveGtid(gtid);
        delayMonitorReport.receiveMonitorGtid(gtid);
        delayMonitorReport.sendGtid(gtid);
        Thread.sleep(10);
    }

    @After
    public void tearDown() throws Exception {
        delayMonitorReport.stop();
        delayMonitorReport.dispose();
    }
}