package com.ctrip.framework.drc.console.enums;

public enum DataMediaPairTypeEnum {
    
    DB_TO_DB(0,"table to table"),
    DB_TO_MQ(1,"table to topic");
    
    
    private Integer type;
    private String comment;

    DataMediaPairTypeEnum(Integer type, String comment) {
        this.type = type;
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public Integer getType() {
        return type;
    }
}
