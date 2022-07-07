package com.ctrip.framework.drc.console.enums;

/**
 * @ClassName HttpRequestEnum
 * @Author haodongPan
 * @Date 2022/6/30 15:37
 * @Version: $
 */
public enum HttpRequestEnum {
    
    GET("HTTP_GET"),
    POST("HTTP_POST"),
    PUT("HTTP_PUT"),
    DELETE("HTTP_DELETE");
    
    
    private String description;
    public String getDescription() {
        return this.description;
    }
    HttpRequestEnum(String description) {
        this.description = description;
    }
}
