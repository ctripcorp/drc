package com.ctrip.framework.drc.core.monitor.kpi;

import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.core.monitor.entity.RowsFilterEntity;
import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.server.common.filter.ExtractType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/6/13
 */
public class OutboundMonitorReportTest {

    private OutboundMonitorReport outboundMonitorReport;

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

        outboundMonitorReport = new OutboundMonitorReport(123l, trafficEntity);
    }

    @Test
    public void testUpdateFilteredRows() {
        outboundMonitorReport.updateFilteredRows("db", "table", 5, 5, ExtractType.ROW);
        outboundMonitorReport.updateFilteredRows("db", "table", 5, 1, ExtractType.ROW);
        Map<TableKey, RowsFilterEntity> rowsFilterEntityMap = outboundMonitorReport.getRowsFilterEntityMap();
        Assert.assertEquals(1, rowsFilterEntityMap.size());
        RowsFilterEntity rowsFilterEntity = rowsFilterEntityMap.get(new TableKey("db", "table"));
        Assert.assertNotNull(rowsFilterEntity);
        long total = rowsFilterEntity.getTotal();
        long send = rowsFilterEntity.getSend();
        Assert.assertEquals(total, 10);
        Assert.assertEquals(send, 6);

        rowsFilterEntity.clearCount();
        total = rowsFilterEntity.getTotal();
        send = rowsFilterEntity.getSend();
        Assert.assertEquals(total, 0);
        Assert.assertEquals(send, 0);
    }

}
