package com.ctrip.framework.drc.console.vo.log;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/9 19:48
 */
public class ConflictTrxLogDetailView {
    private Long conflictTrxLogId;
    private Integer trxResult;
    private String srcDc;
    private String dstDc;
    private List<ConflictRowsLogDetailView> rowsLogDetailViews;

    public Long getConflictTrxLogId() {
        return conflictTrxLogId;
    }

    public void setConflictTrxLogId(Long conflictTrxLogId) {
        this.conflictTrxLogId = conflictTrxLogId;
    }

    public Integer getTrxResult() {
        return trxResult;
    }

    public void setTrxResult(Integer trxResult) {
        this.trxResult = trxResult;
    }

    public String getSrcDc() {
        return srcDc;
    }

    public void setSrcDc(String srcDc) {
        this.srcDc = srcDc;
    }

    public String getDstDc() {
        return dstDc;
    }

    public void setDstDc(String dstDc) {
        this.dstDc = dstDc;
    }

    public List<ConflictRowsLogDetailView> getRowsLogDetailViews() {
        return rowsLogDetailViews;
    }

    public void setRowsLogDetailViews(List<ConflictRowsLogDetailView> rowsLogDetailViews) {
        this.rowsLogDetailViews = rowsLogDetailViews;
    }
}
