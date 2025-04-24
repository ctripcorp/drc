package com.ctrip.framework.drc.service.console.ckafka;

import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/7 15:52
 */
public class KafkaACLCheckResponse {
    private String status;
    private List<KafkaACLInfo> acls;
    private String message;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<KafkaACLInfo> getAcls() {
        return acls;
    }

    public void setAcls(List<KafkaACLInfo> acls) {
        this.acls = acls;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
