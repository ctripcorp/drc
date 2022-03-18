package com.ctrip.framework.drc.core.service.ops;

import java.util.List;
import java.util.Objects;

/**
 * @ClassName AppClusterResult
 * @Author haodongPan
 * @Date 2022/3/17 19:54
 * @Version: $
 */
public class AppClusterResult {

    private String message;

    private Integer total;

    private boolean status;

    private List<AppNode> data;

    public boolean isLegal() {
        if (status && data.size() != 0) {
            for (AppNode node : data) {
                if (!node.isLegal()) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "AppClusterResult{" +
                "message='" + message + '\'' +
                ", total=" + total +
                ", status=" + status +
                ", data=" + data +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppClusterResult that = (AppClusterResult) o;
        return status == that.status && Objects.equals(message, that.message) && Objects.equals(total, that.total) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, total, status, data);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public List<AppNode> getData() {
        return data;
    }

    public void setData(List<AppNode> data) {
        this.data = data;
    }
}