package com.ctrip.framework.drc.core.service.beacon;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-25
 */
public class BeaconResult<T> {

    private Integer code;

    private String msg;

    private T data;

    public static final String BEACON_SUCCESS = "success";

    public static final String BEACON_FAILURE = "failure";

    public static final int BEACON_FAIL_WITHOUT_RETRY = -1;

    public static <T> BeaconResult getInstance(T data, Integer code, String msg) {
        BeaconResult<T> result = new BeaconResult<T>();
        result.setCode(code);
        result.setMsg(msg);
        result.setData(data);
        return result;
    }

    public static <T> BeaconResult getSuccessInstance(T data) {
        return getInstance(data, ResultCode.HANDLE_SUCCESS.getCode(), BEACON_SUCCESS);
    }

    public static <T> BeaconResult getSuccessInstance(T data, String msg) {
        return getInstance(data, ResultCode.HANDLE_SUCCESS.getCode(), msg);
    }

    public static <T> BeaconResult getFailInstance(T data) {
        return getInstance(data, ResultCode.HANDLE_FAIL.getCode(), BEACON_FAILURE);
    }

    public static <T> BeaconResult getFailInstance(T data, String msg) {
        return getInstance(data, ResultCode.HANDLE_FAIL.getCode(), msg);
    }

    public static <T> BeaconResult getFailInstanceWithoutRetry(T data) {
        return getInstance(data, BEACON_FAIL_WITHOUT_RETRY, BEACON_FAILURE);
    }

    public static <T> BeaconResult getFailInstanceWithoutRetry(T data, String msg) {
        return getInstance(data, BEACON_FAIL_WITHOUT_RETRY, msg);
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
