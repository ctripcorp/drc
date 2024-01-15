package com.ctrip.framework.drc.core.monitor.column;

import com.ctrip.framework.drc.core.driver.binlog.impl.ReferenceCountedDelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.core.server.utils.RowsEventUtils;
import com.ctrip.xpipe.api.codec.Codec;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-20
 */
public class DelayMonitorColumn {
    private static final Logger logger = LoggerFactory.getLogger(DelayMonitorColumn.class);

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
        RowsEventUtils.reset(delayMonitorLogEvent.getUpdateRowsEvent());
        delayMonitorLogEvent.load(columns);
        UpdateRowsEvent updateRowsEvent = delayMonitorLogEvent.getUpdateRowsEvent();
        return updateRowsEvent.getAfterPresentRowsValues();
    }

    public static String getDelayMonitorSrcRegionName(ReferenceCountedDelayMonitorLogEvent delayMonitorLogEvent) {
        List<List<Object>> rowValues = getAfterPresentRowsValues(delayMonitorLogEvent);
        List<Object> values = rowValues.get(0);
        return transform((String) values.get(1), (String) values.get(2));
    }

    public static String transform(String src, String dst) {
        try {
            DelayInfo idc = Codec.DEFAULT.decode(dst, DelayInfo.class);
            return idc.getR();
        } catch (Exception e) {
            return src;
        }
    }

    public static boolean match(ReferenceCountedDelayMonitorLogEvent delayMonitorLogEvent) {
        UpdateRowsEvent updateRowsEvent = delayMonitorLogEvent.getUpdateRowsEvent();
        RowsEventUtils.reset(updateRowsEvent);
        try {
            delayMonitorLogEvent.load(columns);
            List<List<Object>> afterPresentRowsValues = updateRowsEvent.getAfterPresentRowsValues();
            return afterPresentRowsValues != null && afterPresentRowsValues.get(0).size() == columns.size();
        } catch (Throwable e) {
            logger.debug("parse match error: " + e.getMessage(), e);
            return false;
        } finally {
            // clean
            RowsEventUtils.reset(updateRowsEvent);
        }
    }
}
