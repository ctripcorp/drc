package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.FileChannel;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;

/**
 * gtid、query、tablemap1、tablemap2、rows1、rows2、xid
 *
 * @Author limingdong
 * @create 2022/4/22
 */
public class TableFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    protected final Logger ROWS_FILTER_LOGGER = LoggerFactory.getLogger("ROWS FILTER");

    private Map<Long, TableMapLogEvent> tableMapWithinTransaction = Maps.newHashMap();  // clear with xid

    private Map<String, TableMapLogEvent> drcTableMap = Maps.newHashMap();  // put every drc_table_map_log_event

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();
        FileChannel fileChannel = value.getFileChannel();
        if (table_map_log_event == eventType || drc_table_map_log_event == eventType) {
            TableMapLogEvent tableMapLogEvent = new TableMapLogEvent();
            TableMapLogEvent previousTableMapLogEvent;
            value.backToHeader();
            EventReader.readEvent(fileChannel, tableMapLogEvent);
            value.setNoRowFiltered(true);
            if (table_map_log_event == eventType) {
                value.getTrafficStatisticKey().setDbName(tableMapLogEvent.getSchemaName());
                logger.info("[flow] table map name is: {}", tableMapLogEvent.getSchemaName());
                previousTableMapLogEvent = tableMapWithinTransaction.put(tableMapLogEvent.getTableId(), tableMapLogEvent);
            } else {
                previousTableMapLogEvent = drcTableMap.put(tableMapLogEvent.getSchemaNameDotTableName(), tableMapLogEvent);
            }
            if (previousTableMapLogEvent != null) {
                String tableName = previousTableMapLogEvent.getSchemaNameDotTableName();
                previousTableMapLogEvent.release();
                ROWS_FILTER_LOGGER.info("[Release] TableMapLogEvent for {} of type {}", tableName, previousTableMapLogEvent.getLogEventType());
            }
            value.restorePosition();
        } else if (LogEventUtils.isRowsEvent(eventType)) {
            value.setTableMapWithinTransaction(tableMapWithinTransaction);
            value.setDrcTableMap(drcTableMap);
        }

        boolean res =  doNext(value, value.isNoRowFiltered());

        if (xid_log_event == eventType) {
            releaseTableMapEvent();  // clear TableMapLogEvent in transaction
            res = true;
            value.setNoRowFiltered(true);
        }

        return res;
    }

    @VisibleForTesting
    public Map<Long, TableMapLogEvent> getTableMapWithinTransaction() {
        return tableMapWithinTransaction;
    }

    @VisibleForTesting
    public Map<String, TableMapLogEvent> getDrcTableMap() {
        return drcTableMap;
    }

    @Override
    public void release() {
        releaseTableMapEvent();
        releaseDrcTableMapEvent();
        ROWS_FILTER_LOGGER.info("[Release] TableMapLogEvent within {}", getClass().getSimpleName());
    }

    private void releaseTableMapEvent() {
        try {
            for (TableMapLogEvent tableMapLogEvent : tableMapWithinTransaction.values()) {
                tableMapLogEvent.release();
            }
            this.tableMapWithinTransaction.clear();
        } catch (Exception e) {
        }
    }

    private void releaseDrcTableMapEvent() {
        try {
            for (TableMapLogEvent tableMapLogEvent : drcTableMap.values()) {
                tableMapLogEvent.release();
            }
            this.drcTableMap.clear();
        } catch (Exception e) {
        }
    }
}
