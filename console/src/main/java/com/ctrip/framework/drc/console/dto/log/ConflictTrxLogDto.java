package com.ctrip.framework.drc.console.dto.log;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/12 19:22
 */
public class ConflictTrxLogDto {
    private String srcMha;
    private String dstMha;
    private String gtid;
    private Integer trxRowsNum;
    private Integer cflRowsNum;
    private Long handleTime;
    private Integer trxRes;
    private List<ConflictRowsLogDto> cflLogs;

    public Integer getTrxRes() {
        return trxRes;
    }

    public void setTrxRes(Integer trxRes) {
        this.trxRes = trxRes;
    }

    public List<ConflictRowsLogDto> getCflLogs() {
        return cflLogs;
    }

    public void setCflLogs(List<ConflictRowsLogDto> cflLogs) {
        this.cflLogs = cflLogs;
    }

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

    public Long getHandleTime() {
        return handleTime;
    }

    public void setHandleTime(Long handleTime) {
        this.handleTime = handleTime;
    }

}
