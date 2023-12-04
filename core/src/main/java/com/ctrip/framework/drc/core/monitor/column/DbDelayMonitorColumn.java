package com.ctrip.framework.drc.core.monitor.column;

import com.ctrip.framework.drc.core.driver.binlog.impl.ReferenceCountedDelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.core.server.utils.RowsEventUtils;
import com.google.common.collect.Lists;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * for drc db delay
 */
public class DbDelayMonitorColumn {
    private static final Logger logger = LoggerFactory.getLogger(DbDelayMonitorColumn.class);

    private static List<TableMapLogEvent.Column> columns = Lists.newArrayList();
    private static ThreadLocal<SimpleDateFormat> dateFormatThreadLocal = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));

    public static final String CREATE_DELAY_TABLE_SQL = "CREATE TABLE IF NOT EXISTS `drcmonitordb`.`%s` (\n" +
            "  `id` bigint NOT NULL,\n" +
            "  `delay_info` varchar(256) NOT NULL,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb3";

    static {
        TableMapLogEvent.Column columnId = new TableMapLogEvent.Column("id", false, "bigint", null, "19", "0", null, null, null, "bigint(20)", "PRI", "auto_increment", null);
        TableMapLogEvent.Column delayInfo = new TableMapLogEvent.Column("delay_info", false, "varchar", "768", null, null, null, "utf8mb3", "utf8mb3_general_ci", "varchar(256)", "", "", null);
        TableMapLogEvent.Column columnUpdateTime = new TableMapLogEvent.Column("datachange_lasttime", false, "timestamp", null, null, null, "3", null, null, "timestamp(3)", "", "on update CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3)");
        columns.add(columnId);
        columns.add(delayInfo);
        columns.add(columnUpdateTime);
    }

    public static List<List<Object>> getAfterPresentRowsValues(ReferenceCountedDelayMonitorLogEvent delayMonitorLogEvent) {
        UpdateRowsEvent updateRowsEvent = delayMonitorLogEvent.getUpdateRowsEvent();
        RowsEventUtils.reset(updateRowsEvent);
        delayMonitorLogEvent.load(columns);
        return updateRowsEvent.getAfterPresentRowsValues();
    }

    public static boolean match(ReferenceCountedDelayMonitorLogEvent delayMonitorLogEvent) {
        try {
            DbDelayDto dbDelayDto = parseEvent(delayMonitorLogEvent);
            return !StringUtils.isEmpty(dbDelayDto.getDcName());
        } catch (Throwable e) {
            logger.debug("parse match v2 error: " + e.getMessage(), e);
            return false;
        }
    }


    public static String getDelayMonitorSrcDcName(ReferenceCountedDelayMonitorLogEvent delayMonitorLogEvent) {
        try {
            return parseEvent(delayMonitorLogEvent).getDcName();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static DbDelayDto parseEvent(ReferenceCountedDelayMonitorLogEvent delayMonitorLogEvent) throws ParseException {
        List<List<Object>> rowValues = getAfterPresentRowsValues(delayMonitorLogEvent);
        List<Object> values = rowValues.get(0);
        long id = (long) values.get(0);
        String json = (String) values.get(1);
        String timeString = (String) values.get(2);
        return DbDelayDto.from(
                id,
                DbDelayDto.DelayInfo.parse(json),
                dateFormatThreadLocal.get().parse(timeString).getTime()
        );
    }

    public static String getUpdateRowsBytes(UpdateRowsEvent updateRowsEvent) {
        RowsEventUtils.reset(updateRowsEvent);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, updateRowsEvent.getLogEventHeader().getHeaderBuf(), updateRowsEvent.getPayloadBuf());

        StringBuilder sb = new StringBuilder();
        List<Byte> list = new ArrayList<>();
        for (int i = 0; i < compositeByteBuf.numComponents(); i++) {
            compositeByteBuf.component(i).forEachByte(b -> {
                list.add(b);
                return true;
            });
        }
        sb.append("byte[] bytes = new byte[] {\n");
        for (Byte b : list.subList(0, 19)) {
            sb.append(String.format("(byte) 0x%02x, ", b));
        }
        sb.append("\n");
        List<Byte> body = list.subList(19, list.size());
        List<List<Byte>> partition = Lists.partition(body, 16);
        for (List<Byte> bytes : partition) {
            for (Byte b : bytes) {
                sb.append(String.format("(byte) 0x%02X, ", b));
            }
            sb.append("\n");
        }


        sb.append("};");

        System.out.println(sb.toString());
        return sb.toString();
    }
}
