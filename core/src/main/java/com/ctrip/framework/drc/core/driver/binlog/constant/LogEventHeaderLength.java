package com.ctrip.framework.drc.core.driver.binlog.constant;

/**
 * Created by @author zhuYongMing on 2019/9/6.
 */
public class LogEventHeaderLength {

    // if binlog version == 1, header length == 13; else if version > 1, header length == 19
    public static final int eventHeaderLengthVersionEq1 = 13;

    public static final int eventHeaderLengthVersionGt1 = 19;
}
