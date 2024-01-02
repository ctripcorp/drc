package com.ctrip.framework.drc.console.enums.log;/**
 *@ClassName LogBlackTableType
 *@Author haodongPan
 *@Date 2023/11/27 19:10 
 *@Version:  $
 */
public enum LogBlackListType {
    USER(0, "user add blacklist"),
    NEW_CONFIG(1, "new config table in drc,auto add blacklist"),
    DBA_JOB(2, "dba touch job,add to blacklist"),
    ALARM_HOTSPOT(3, "alarm too many times and no one care,add to blacklist"),
    ;
    private int code;
    private String desc;

    LogBlackListType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

}
