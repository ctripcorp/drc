package com.ctrip.framework.drc.console.param.filter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/5/8 20:43
 */
public class QConfigBaseParam {
    private String token;
    private String serverEnv;
    private String groupId;
    private String operator;

    public Map<String, String> extractMap() {
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("token", this.getToken());
        paramMap.put("operator", this.getOperator());
        paramMap.put("serverenv", this.getServerEnv());
        paramMap.put("groupid", this.getGroupId());

        return paramMap;
    }

    @Override
    public String toString() {
        return "QConfigBaseParam{" +
                "token='" + token + '\'' +
                ", serverEnv='" + serverEnv + '\'' +
                ", groupId='" + groupId + '\'' +
                ", operator='" + operator + '\'' +
                '}';
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getServerEnv() {
        return serverEnv;
    }

    public void setServerEnv(String serverEnv) {
        this.serverEnv = serverEnv;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }
}
