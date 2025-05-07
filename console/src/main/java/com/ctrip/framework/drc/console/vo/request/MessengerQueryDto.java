package com.ctrip.framework.drc.console.vo.request;

import com.ctrip.framework.drc.core.mq.MqType;

public class MessengerQueryDto {
    private MhaQueryDto mha;
    private String dbNames;
    private Integer drcStatus;
    private String topic;
    private String mqType;


    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public MqType getMqTypeEnum() {
        MqType parse = MqType.parse(mqType);
        if (parse == null) {
            // default
            parse = MqType.DEFAULT;
        }
        return parse;
    }

    public void validate() {
        if (mqType == null) {
            throw new IllegalArgumentException("mqType is null");
        }
        if (getMqTypeEnum() == null) {
            throw new IllegalArgumentException("mqType: " + mqType + " is invalid");
        }
    }
}
