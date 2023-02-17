package com.ctrip.framework.drc.console.service.remote.qconfig.response;

import java.util.List;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName SubEnvResponse
 * @Author haodongPan
 * @Date 2023/2/9 11:28
 * @Version: $
 * API 5.5.3
 */
public class SubEnvResponse {

    private Integer status;

    private String message;

    private List<String> data;
    
    public boolean isLegal() {
        return 0 == status && !CollectionUtils.isEmpty(data);
    }

    @Override
    public String toString() {
        return "SubEnvResponse{" +
                "status=" + status +
                ", message='" + message + '\'' +
                ", data=" + data +
                '}';
    }

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

    public List<String> getData() {
        return data;
    }

    public void setData(List<String> data) {
        this.data = data;
    }
}
