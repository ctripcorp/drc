package com.ctrip.framework.drc.console.enums;

public enum ReplicationTypeEnum {
    
    DB_TO_DB(0,"table to table"),
    DB_TO_MQ(1,"table to topic");
    
    
    private Integer type;
    private String comment;

    ReplicationTypeEnum(Integer type, String comment) {
        this.type = type;
        this.comment = comment;
    }

    public static ReplicationTypeEnum getByType(Integer type) {
        for (ReplicationTypeEnum value : ReplicationTypeEnum.values()) {
            if (value.type.equals(type)) {
                return value;
            }
        }
        throw new IllegalArgumentException(String.format("unknown ReplicationTypeEnum: %s", type));
    }

    public String getComment() {
        return comment;
    }

    public Integer getType() {
        return type;
    }
}
