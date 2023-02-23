package com.ctrip.framework.drc.service.console.dbcluster;

import java.util.List;

/**
 * @ClassName DbInfosResponse
 * @Author haodongPan
 * @Date 2023/2/15 20:27
 * @Version: $
 */
public class DbInfosResponse {
    
    private Integer status;
    
    private String message;
    
    private List<DbInfoDetail> result;


    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<DbInfoDetail> getResult() {
        return result;
    }

    public void setResult(List<DbInfoDetail> result) {
        this.result = result;
    }
}
