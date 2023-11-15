package com.ctrip.framework.drc.core.monitor.operator;

/**
 * Created by dengquanliang
 * 2023/11/15 10:51
 */
public class StatementExecutorResult {
    /**
     * 0-success 1-fail
     */
    private int result;
    private String message;

    public StatementExecutorResult() {
    }

    public StatementExecutorResult(int result, String message) {
        this.message = message;
        this.result = result;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }
}
