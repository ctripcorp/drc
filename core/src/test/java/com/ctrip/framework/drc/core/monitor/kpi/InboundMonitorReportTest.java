package com.ctrip.framework.drc.core.monitor.kpi;

import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by dengquanliang
 * 2025/10/29 16:41
 */
public class InboundMonitorReportTest {
    private InboundMonitorReport inboundMonitorReport;

    private TrafficEntity trafficEntity;

    @Before
    public void setUp() {
        trafficEntity = new TrafficEntity.Builder()
                .clusterAppId(123l)
                .buName("bu")
                .dcName("dc")
                .clusterName("cluster")
                .ip("1.1.1.1")
                .port(123)
                .direction("desc")
                .module("in")
                .build();

        inboundMonitorReport = new InboundMonitorReport(123l, trafficEntity);
    }

    @Test
    public void doMonitor() {
        inboundMonitorReport.addDb("db", "gtid");
        inboundMonitorReport.addTable("table");
        inboundMonitorReport.addDbFilter("db");
        inboundMonitorReport.addGhostDbFilter("db");

        inboundMonitorReport.doMonitor();
    }
}
