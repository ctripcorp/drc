package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.core.mq.MqType;

import java.io.Serializable;
import java.util.List;


public class MessengerDelayQueryDto implements Serializable {
    private List<String> mhas;
    private List<String> dbs;
    private Boolean noNeedDbAndSrcTime;
    private String mqType;


    public MqType getMqTypeEnum() {
        MqType mqTypeEnum = MqType.parse(mqType);
        if(mqTypeEnum == null){
            mqTypeEnum = MqType.DEFAULT;
        }
        return mqTypeEnum;
    }
    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public void setNoNeedDbAndSrcTime(Boolean noNeedDbAndSrcTime) {
        this.noNeedDbAndSrcTime = noNeedDbAndSrcTime;
    }

    public boolean getNoNeedDbAndSrcTime() {
        return Boolean.TRUE.equals(noNeedDbAndSrcTime);
    }

    public List<String> getMhas() {
        return mhas;
    }

    public void setMhas(List<String> mhas) {
        this.mhas = mhas;
    }

    public List<String> getDbs() {
        return dbs;
    }

    public void setDbs(List<String> dbs) {
        this.dbs = dbs;
    }
}
