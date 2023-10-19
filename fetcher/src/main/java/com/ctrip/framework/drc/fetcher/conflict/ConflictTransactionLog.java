package com.ctrip.framework.drc.fetcher.conflict;

import java.util.List;

/**
 * @ClassName ConflictTransaction
 * @Author haodongPan
 * @Date 2023/9/25 19:11
 * @Version: $
 */

public class ConflictTransactionLog {
    private String srcMha;
    private String dstMha;
    private String gtid;
    private Long trxRowsNum;
    private Long cflRowsNum;
    private Long handleTime;
    private List<ConflictRowLog> cflLogs;
    private Integer trxRes; // 0-commit 1-rollback
    
    
    public String getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public String getDstMha() {
        return dstMha;
    }

    public void setDstMha(String dstMha) {
        this.dstMha = dstMha;
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public Long getTrxRowsNum() {
        return trxRowsNum;
    }

    public void setTrxRowsNum(Long trxRowsNum) {
        this.trxRowsNum = trxRowsNum;
    }

    public Long getCflRowsNum() {
        return cflRowsNum;
    }

    public void setCflRowsNum(Long cflRowsNum) {
        this.cflRowsNum = cflRowsNum;
    }

    public Integer getTrxRes() {
        return trxRes;
    }

    public void setTrxRes(Integer trxRes) {
        this.trxRes = trxRes;
    }

    public Long getHandleTime() {
        return handleTime;
    }

    public void setHandleTime(Long handleTime) {
        this.handleTime = handleTime;
    }

    public List<ConflictRowLog> getCflLogs() {
        return cflLogs;
    }

    public void setCflLogs(List<ConflictRowLog> cflLogs) {
        this.cflLogs = cflLogs;
    }
}
