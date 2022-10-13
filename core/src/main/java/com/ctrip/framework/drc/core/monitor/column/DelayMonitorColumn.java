package com.ctrip.framework.drc.core.monitor.column;

import com.ctrip.framework.drc.core.driver.binlog.impl.ReferenceCountedDelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.xpipe.api.codec.Codec;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-20
 */
public class DelayMonitorColumn {
    private static List<TableMapLogEvent.Column> columns = Lists.newArrayList();

    static  {
        TableMapLogEvent.Column columnId = new TableMapLogEvent.Column("id", false, "bigint", null, "19", "0", null, null, null,"bigint(20)", "PRI", "auto_increment", null);
        TableMapLogEvent.Column columnSrcIp = new TableMapLogEvent.Column("src_ip", false, "varchar", "15", null, null, null,"latin1", "latin1_swedish_ci", "varchar(15)", "", "", null);
        TableMapLogEvent.Column columnDestIp = new TableMapLogEvent.Column("dest_ip", false, "varchar", "256", null, null, null, "latin1", "latin1_swedish_ci", "varchar(256)", "", "", null);
        TableMapLogEvent.Column columnUpdateTime = new TableMapLogEvent.Column("datachange_lasttime", false, "timestamp", null, null, null, "3", null, null, "timestamp(3)", "", "on update CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3)");
        columns.add(columnId);
        columns.add(columnSrcIp);
        columns.add(columnDestIp);
        columns.add(columnUpdateTime);
    }

    public static List<List<Object>> getAfterPresentRowsValues(ReferenceCountedDelayMonitorLogEvent delayMonitorLogEvent) {
        delayMonitorLogEvent.getUpdateRowsEvent().getPayloadBuf().readerIndex(0);
        delayMonitorLogEvent.load(columns);
        UpdateRowsEvent updateRowsEvent = delayMonitorLogEvent.getUpdateRowsEvent();
        return updateRowsEvent.getAfterPresentRowsValues();
    }

    public static String getDelayMonitorSrcDcName(ReferenceCountedDelayMonitorLogEvent delayMonitorLogEvent) {
        List<List<Object>> rowValues = getAfterPresentRowsValues(delayMonitorLogEvent);
        List<Object> values = rowValues.get(0);
        return transform((String) values.get(1));
    }

    public static String transform(String locationString) {
        try {
            Idc idc = Codec.DEFAULT.decode(locationString, Idc.class);
            return idc.getRegion();
        } catch (Exception e) {
            return locationString;
        }
    }

}
