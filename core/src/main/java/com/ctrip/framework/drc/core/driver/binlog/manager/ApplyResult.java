package com.ctrip.framework.drc.core.driver.binlog.manager;

/**
 * @Author limingdong
 * @create 2022/11/9
 */
public class ApplyResult {

    private Status status;

    private String ddl;

    public ApplyResult(Status status, String ddl) {
        this.status = status;
        this.ddl = ddl;
    }

    public static ApplyResult from(Status key, String value) {
        return new ApplyResult(key, value);
    }

    public Status getStatus() {
        return status;
    }

    public String getDdl() {
        return ddl;
    }

    public enum Status {

        SUCCESS,

        FAIL,

        PARTITION_SKIP
    }

    @Override
    public String toString() {
        return "ApplyResult{" +
                "status=" + status +
                ", ddl='" + ddl + '\'' +
                '}';
    }
}
