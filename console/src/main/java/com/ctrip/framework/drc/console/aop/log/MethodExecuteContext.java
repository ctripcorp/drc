package com.ctrip.framework.drc.console.aop.log;


import com.ctrip.framework.drc.console.dao.log.entity.OperationLogTbl;

/**
 * @ClassName MethodExecuteResult
 * @Author haodongPan
 * @Date 2023/12/6 20:09
 * @Version: $
 */
public class MethodExecuteContext {
    private boolean success;
    private Throwable throwable;
    private String errorMsg;
    private Object result;
    private OperationLogTbl operationLogTbl;
    
    public MethodExecuteContext() {
        operationLogTbl = new OperationLogTbl();
    }

    public OperationLogTbl getOperationLogTbl() {
        return operationLogTbl;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public boolean isSuccess() {
        return success;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public Object getResult() {
        return result;
    }
}