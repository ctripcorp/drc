package com.ctrip.framework.drc.console.vo.display.v2;

import com.ctrip.framework.drc.console.dto.v2.MhaDbDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;

public class DelayInfoVo {

    String dbName;
    String srcMha;
    String dstMha;
    private Long delay;

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

    public Long getDelay() {
        return delay;
    }

    public void setDelay(Long delay) {
        this.delay = delay;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
    public static DelayInfoVo from(MhaDelayInfoDto dto) {
        DelayInfoVo vo = new DelayInfoVo();
        vo.setDelay(dto.getDelay());
        vo.setDstMha(dto.getDstMha());
        vo.setSrcMha(dto.getSrcMha());
        return vo;
    }

    public static DelayInfoVo from(MhaDbDelayInfoDto dto) {
        DelayInfoVo vo = new DelayInfoVo();
        vo.setDelay(dto.getDelay());
        vo.setDstMha(dto.getDstMha());
        vo.setSrcMha(dto.getSrcMha());
        vo.setDbName(dto.getDbName());
        return vo;
    }

}
