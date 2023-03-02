package com.ctrip.framework.drc.console.service.remote.qconfig.request;

import java.util.Map;

/**
 * @ClassName CreateFileRequestBody
 * @Author haodongPan
 * @Date 2023/2/9 15:23
 * @Version: $
 */
public class CreateFileRequestBody {
    
    //yyyy-MM-dd HHmmss
    private String requesttime;
    private String operator;
    private Map<String,Object> config;
    
    // configs
    //    private String groupid;
    //    private String targetgroupid;
    //    private String env;
    //    private String subenv;
    //    private String serverenv;
    //    private String serversubenv;
    //    private String dataid;
    //    private Integer version;
    //    private Integer content;
    //    private String public;
    //    private String description;
    
    @Override
    public String toString() {
        return "CreateFileRequestBody{" +
                "requesttime='" + requesttime + '\'' +
                ", operator='" + operator + '\'' +
                ", config=" + config +
                '}';
    }

    public String getRequesttime() {
        return requesttime;
    }

    public void setRequesttime(String requesttime) {
        this.requesttime = requesttime;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}
