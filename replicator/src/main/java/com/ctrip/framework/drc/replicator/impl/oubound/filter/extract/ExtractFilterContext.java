package com.ctrip.framework.drc.replicator.impl.oubound.filter.extract;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;

import java.util.List;

/**
 * Created by jixinwang on 2022/12/29
 */
public class ExtractFilterContext {

    private boolean needExtractRows;

    private boolean needExtractColumns;

    private boolean rewrite;

    private LogEventType eventType;

    private LogEvent logEvent;

    private String tableName;

    private TableMapLogEvent tableMapLogEvent;

    private TableMapLogEvent drcTableMapLogEvent;

    private List<Integer> extractedColumnsIndex;

    private RowsFilterContext rowsFilterContext = new RowsFilterContext();

    private AbstractRowsEvent rowsEvent;

    private String gtid;

    public boolean needExtractRows() {
        return needExtractRows;
    }

    public void setNeedExtractRows(boolean needExtractRows) {
        this.needExtractRows = needExtractRows;
    }

    public boolean needExtractColumns() {
        return needExtractColumns;
    }

    public void setNeedExtractColumns(boolean needExtractColumns) {
        this.needExtractColumns = needExtractColumns;
    }

    public boolean isRewrite() {
        return rewrite;
    }

    public void setRewrite(boolean rewrite) {
        this.rewrite = rewrite;
    }

    public LogEventType getEventType() {
        return eventType;
    }

    public void setEventType(LogEventType eventType) {
        this.eventType = eventType;
    }

    public LogEvent getLogEvent() {
        return logEvent;
    }

    public void setLogEvent(LogEvent logEvent) {
        this.logEvent = logEvent;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public TableMapLogEvent getTableMapLogEvent() {
        return tableMapLogEvent;
    }

    public void setTableMapLogEvent(TableMapLogEvent tableMapLogEvent) {
        this.tableMapLogEvent = tableMapLogEvent;
    }

    public TableMapLogEvent getDrcTableMapLogEvent() {
        return drcTableMapLogEvent;
    }

    public void setDrcTableMapLogEvent(TableMapLogEvent drcTableMapLogEvent) {
        this.drcTableMapLogEvent = drcTableMapLogEvent;
    }

    public List<Integer> getExtractedColumnsIndex() {
        return extractedColumnsIndex;
    }

    public void setExtractedColumnsIndex(List<Integer> extractedColumnsIndex) {
        this.extractedColumnsIndex = extractedColumnsIndex;
    }

    public RowsFilterContext getRowsFilterContext() {
        return rowsFilterContext;
    }

    public void setRowsFilterContext(RowsFilterContext rowsFilterContext) {
        this.rowsFilterContext = rowsFilterContext;
    }

    public AbstractRowsEvent getRowsEvent() {
        return rowsEvent;
    }

    public void setRowsEvent(AbstractRowsEvent rowsEvent) {
        this.rowsEvent = rowsEvent;
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public void clear() {
        if (rowsFilterContext != null) {
            rowsFilterContext.clear();
        }
    }

    public void reset(boolean needExtractRows, boolean needExtractColumns, TableMapLogEvent drcTableMapLogEvent,
                      AbstractRowsEvent rowsEvent, String gtid, List<Integer> extractedColumnsIndex) {
        this.needExtractRows = needExtractRows;
        this.needExtractColumns = needExtractColumns;
        this.drcTableMapLogEvent = drcTableMapLogEvent;
        this.rowsEvent = rowsEvent;
        this.gtid = gtid;
        this.extractedColumnsIndex = extractedColumnsIndex;
    }
}
