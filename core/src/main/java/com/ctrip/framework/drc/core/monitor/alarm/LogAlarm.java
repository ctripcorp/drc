package com.ctrip.framework.drc.core.monitor.alarm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/10/9 下午11:47.
 */
public class LogAlarm implements Alarm {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void alarm(String content) {
        logger.error(">>>>>>>>>>>> [LogAlarm] {}", content);
    }
}
