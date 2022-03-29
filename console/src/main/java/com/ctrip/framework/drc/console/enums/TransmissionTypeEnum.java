package com.ctrip.framework.drc.console.enums;

/**
 * @ClassName DrcTypeEnum
 * @Author haodongPan
 * @Date 2022/3/16 19:34
 * @Version: $
 */
public enum TransmissionTypeEnum {
    
    DUPLEX("duplex") {},
    SIMPLEX("simplex") {},
    NOCONFIG("noConfig"){};
    
    private String transmissionType;
    
    TransmissionTypeEnum(String transmissionType) {
        this.transmissionType = transmissionType;
    }

    public String getType() {
        return transmissionType;
    }
}
