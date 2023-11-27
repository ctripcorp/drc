package com.ctrip.framework.drc.console.utils;

/**
 * Created by dengquanliang
 * 2023/11/14 14:41
 */
public class Constants {

    public static final String SUCCESS = "success";
    public static final String FAIL = "fail";
    public static final String BEGIN = "begin;";
    public static final String COMMIT = "commit;";
    public static final String ENDPOINT_NOT_EXIST = "endpoint not exist";
    public static final String CONFLICT_SQL_PREFIX = "/*DRC HANDLE CONFLICT*/";

    public static final Long ONE_HOUR = 60 * 60 * 1000L;
    public static final Long TWO_HOUR = 2 * 60 * 60 * 1000L;
    public static final Long FIVE_MINUTE = 5 * 60000L;

}
