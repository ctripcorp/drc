package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.core.mq.MqType;

import java.util.List;

/**
 * @author: yongnian
 * @create: 2024/6/20 11:49
 */
public class MessengerSwitchReqDto {
    private List<String> dbNames;
    private String srcMhaName;
    private String mqType;
    private boolean switchOnly;

    public List<String> getDbNames() {
        return dbNames;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }


    public void setDbNames(List<String> dbNames) {
        this.dbNames = dbNames;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }


    public MqType getMqTypeEnum() {
        return MqType.valueOf(mqType);
    }
    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public boolean isSwitchOnly() {
        return switchOnly;
    }

    public void setSwitchOnly(boolean switchOnly) {
        this.switchOnly = switchOnly;
    }
}
