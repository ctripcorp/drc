package com.ctrip.framework.drc.console.enums;

public enum TokenType {
    OPEN_API_4_DRC_ADMIN("openApi4DrcAdmin","open api token for drc admin"),
    OPEN_API_4_DBA("openApi4DBA","open api token for dba");

    String code;
    String desc;
    
    TokenType(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }
    
    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
