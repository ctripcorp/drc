package com.ctrip.framework.drc.console.param.api;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/11/6 15:26
 */
public class ApprovalOpenApiRequest {
    private List<String> approvers;
    private String data;
    private String username;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public List<String> getApprovers() {
        return approvers;
    }

    public void setApprovers(List<String> approvers) {
        this.approvers = approvers;
    }
}

