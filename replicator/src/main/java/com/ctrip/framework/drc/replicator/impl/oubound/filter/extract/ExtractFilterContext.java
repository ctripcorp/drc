package com.ctrip.framework.drc.replicator.impl.oubound.filter.extract;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;

import java.util.List;

/**
 * Created by jixinwang on 2022/12/29
 */
public class ExtractFilterContext {

    private boolean rowsExtracted = false;

    private boolean columnsExtracted = false;

    private TableMapLogEvent drcTableMapLogEvent;

    private List<Integer> extractedColumnsIndex;

    private RowsFilterContext rowsFilterContext;

    private AbstractRowsEvent rowsEvent;

    private String gtid;

    public boolean getRowsExtracted() {
        return rowsExtracted;
    }

    public void setRowsExtracted(boolean rowsExtracted) {
        this.rowsExtracted = rowsExtracted;
    }

    public boolean getColumnsExtracted() {
        return columnsExtracted;
    }

    public void setColumnsExtracted(boolean columnsExtracted) {
        this.columnsExtracted = columnsExtracted;
    }

    public boolean extracted() {
        return rowsExtracted || columnsExtracted;
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
}
