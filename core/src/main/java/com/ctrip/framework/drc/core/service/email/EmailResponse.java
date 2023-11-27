package com.ctrip.framework.drc.core.service.email;

import java.util.List;

/**
 * @ClassName EmailResponse
 * @Author haodongPan
 * @Date 2023/11/23 14:28
 * @Version: $
 */
public class EmailResponse {
    private boolean success;
    private String message;
    private List<String> emailIdList;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<String> getEmailIdList() {
        return emailIdList;
    }

    public void setEmailIdList(List<String> emailIdList) {
        this.emailIdList = emailIdList;
    }
}

