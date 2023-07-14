package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2023/5/5 14:36
 */
public enum HttpRequestParamEnum {
    PATH_VARIABLE(1, "PathVariable"),
    REQUEST_BODY(2, "RequestBody"),
    REQUEST_PARAM(3, "RequestParam"),
    ;

    private int code;
    private String name;

    HttpRequestParamEnum(int code, String name) {
        this.code = code;
        this.name = name;
    }
}
