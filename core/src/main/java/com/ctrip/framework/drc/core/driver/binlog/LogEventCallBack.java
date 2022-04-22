package com.ctrip.framework.drc.core.driver.binlog;

/**
 * @Author limingdong
 * @create 2020/6/30
 */
public interface LogEventCallBack extends HeartBeatCallBack {

    default void onSuccess() {

    }

    default void onFailure() {

    }
}
