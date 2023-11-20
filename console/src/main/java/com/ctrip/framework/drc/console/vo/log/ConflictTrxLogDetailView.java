package com.ctrip.framework.drc.console.vo.log;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/9 19:48
 */
public class ConflictTrxLogDetailView {
    private Long conflictTrxLogId;
    private Integer trxResult;
    private String srcRegion;
    private String dstRegion;
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

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }

    public List<ConflictRowsLogDetailView> getRowsLogDetailViews() {
        return rowsLogDetailViews;
    }

    public void setRowsLogDetailViews(List<ConflictRowsLogDetailView> rowsLogDetailViews) {
        this.rowsLogDetailViews = rowsLogDetailViews;
    }
}
