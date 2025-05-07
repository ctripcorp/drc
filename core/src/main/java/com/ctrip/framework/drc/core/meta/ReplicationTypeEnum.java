package com.ctrip.framework.drc.core.meta;

public enum ReplicationTypeEnum {

    DB_TO_DB(0, "table to table"),
    DB_TO_MQ(1, "table to qmq topic"),
    DB_TO_KAFKA(2, "table to kafka topic"),
    ;

    private final Integer type;
    private final String comment;

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

    public boolean isMqType(){
        return this == DB_TO_MQ || this == DB_TO_KAFKA;
    }
}
