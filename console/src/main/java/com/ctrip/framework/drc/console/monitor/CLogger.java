package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.enums.LogTypeEnum;

import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-27
 */
public interface CLogger {

    void cLog(Map<String, String> tags, String msg, LogTypeEnum type, Throwable t);

    String getCLogPrefix(Map<String, String> tags);
}
