package com.ctrip.framework.drc.console.service.remote.qconfig.response;

/**
 * @ClassName FileDetailResponse
 * @Author haodongPan
 * @Date 2023/2/9 11:31
 * @Version: $
 * API 5.4.1
 */
public class FileDetailResponse {
    private Integer status;

    private String message;

    private FileDetailData data;

    public boolean isExist(){
        return status == 0;
    }

    @Override
    public String toString() {
        return "FileDetailResponse{" +
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

    public FileDetailData getData() {
        return data;
    }

    public void setData(FileDetailData data) {
        this.data = data;
    }
}
