package com.ctrip.framework.drc.console.service.log;/**
 *@ClassName LogBlackTableType
 *@Author haodongPan
 *@Date 2023/11/27 19:10 
 *@Version:  $
 */
public enum LogBlackListType {
    USER(0, "user add blacklist"),
    AUTO(1, "new config drc,auto add blacklist");
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
