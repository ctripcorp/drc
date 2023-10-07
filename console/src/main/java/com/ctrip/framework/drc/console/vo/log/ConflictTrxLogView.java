package com.ctrip.framework.drc.console.vo.log;

/**
 * Created by dengquanliang
 * 2023/9/26 14:35
 */
public class ConflictTrxLogView {
    private Long conflictTrxLogId;
    private String srcMhaName;
    private String dstMhaName;
    private String gtid;
    private Integer trxRowsNum;
    private Integer cflRowsNum;
    private Integer trxResult;
    private String handleTime;

    public Long getConflictTrxLogId() {
        return conflictTrxLogId;
    }

    public void setConflictTrxLogId(Long conflictTrxLogId) {
        this.conflictTrxLogId = conflictTrxLogId;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public void setDstMhaName(String dstMhaName) {
        this.dstMhaName = dstMhaName;
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public Integer getTrxRowsNum() {
        return trxRowsNum;
    }

    public void setTrxRowsNum(Integer trxRowsNum) {
        this.trxRowsNum = trxRowsNum;
    }

    public Integer getCflRowsNum() {
        return cflRowsNum;
    }

    public void setCflRowsNum(Integer cflRowsNum) {
        this.cflRowsNum = cflRowsNum;
    }

    public Integer getTrxResult() {
        return trxResult;
    }

    public void setTrxResult(Integer trxResult) {
        this.trxResult = trxResult;
    }

    public String getHandleTime() {
        return handleTime;
    }

    public void setHandleTime(String handleTime) {
        this.handleTime = handleTime;
    }
}
