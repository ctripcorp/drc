package com.ctrip.framework.drc.console.vo.request;

public class MessengerQueryDto {
    private MhaQueryDto mha;
    private String dbNames;
    private Integer drcStatus;

    public MhaQueryDto getMha() {
        return mha;
    }

    public void setMha(MhaQueryDto mha) {
        this.mha = mha;
    }

    public String getDbNames() {
        return dbNames;
    }

    public void setDbNames(String dbNames) {
        this.dbNames = dbNames;
    }

    public Integer getDrcStatus() {
        return drcStatus;
    }

    public void setDrcStatus(Integer drcStatus) {
        this.drcStatus = drcStatus;
    }
}