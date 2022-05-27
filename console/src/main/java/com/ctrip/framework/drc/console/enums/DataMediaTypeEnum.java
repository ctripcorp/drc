package com.ctrip.framework.drc.console.enums;

/**
 * @ClassName DataMediaTypeEnum
 * @Author haodongPan
 * @Date 2022/4/29 16:00
 * @Version: $
 */
public enum DataMediaTypeEnum {
    ROWS_FILTER(0,"for rows Filter"),
    NAME_MAPPING(1,"for table Mapping");

    private Integer type;
    private String comment;
    
    DataMediaTypeEnum(Integer type, String comment) {
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
