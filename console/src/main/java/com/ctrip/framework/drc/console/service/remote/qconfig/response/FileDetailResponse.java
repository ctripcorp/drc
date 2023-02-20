package com.ctrip.framework.drc.console.service.remote.qconfig.response;

/**
 * @ClassName FileDetailResponse
 * @Author haodongPan
 * @Date 2023/2/9 11:31
 * @Version: $
 * API 5.4.1
 */
public class FileDetailResponse {
    private int status;

    private String message;

    private FileDetailData data;

    public FileDetailResponse() {
    }

    public FileDetailResponse(int status) {
        this.status = status;
    }

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

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
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
