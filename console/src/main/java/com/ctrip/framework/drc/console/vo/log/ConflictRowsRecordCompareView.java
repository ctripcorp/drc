package com.ctrip.framework.drc.console.vo.log;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/25 17:17
 */
public class ConflictRowsRecordCompareView {
    private List<ConflictRowsRecordDetail> recordDetailList;
    /**
     * unEqualRowLogIds
     */
    private List<Long> rowLogIds;

    public List<ConflictRowsRecordDetail> getRecordDetailList() {
        return recordDetailList;
    }

    public void setRecordDetailList(List<ConflictRowsRecordDetail> recordDetailList) {
        this.recordDetailList = recordDetailList;
    }

    public List<Long> getRowLogIds() {
        return rowLogIds;
    }

    public void setRowLogIds(List<Long> rowLogIds) {
        this.rowLogIds = rowLogIds;
    }
}
