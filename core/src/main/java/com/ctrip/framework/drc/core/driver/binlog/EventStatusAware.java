package com.ctrip.framework.drc.core.driver.binlog;

/**
 * @Author limingdong
 * @create 2020/8/4
 */
public interface EventStatusAware {

    void setHalfEvent(boolean halfEvent);
}
