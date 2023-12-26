package com.ctrip.framework.drc.console.vo.log;

/**
 * Created by dengquanliang
 * 2023/12/8 15:06
 */
public class ConflictRowRecordCompareEqualView {

    private Long rowLogId;
    private Boolean recordIsEqual;

    public Long getRowLogId() {
        return rowLogId;
    }

    public void setRowLogId(Long rowLogId) {
        this.rowLogId = rowLogId;
    }

    public Boolean getRecordIsEqual() {
        return recordIsEqual;
    }

    public void setRecordIsEqual(Boolean recordIsEqual) {
        this.recordIsEqual = recordIsEqual;
    }
}
