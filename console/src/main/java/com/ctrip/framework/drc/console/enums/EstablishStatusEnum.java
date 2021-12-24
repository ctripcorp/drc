package com.ctrip.framework.drc.console.enums;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-13
 */
public enum EstablishStatusEnum {

    UNSTART(-1, "not start to access DRC"),

    SENT_NEW_MHA_REQUEST(5, "sent request to DBA api for creating new mha"),

    BUILT_NEW_MHA(10, "built new mha"),

    CUT_REPLICATION(20, "cut replication from original mha to created new mha"),

    CONFIGURED_DB_DOMAIN_NAME(30, "configured the domain name for DBs in original and new mhas thru DBA"),

    MODIFIED_ORIGINAL_DB_DOMAIN_NAME(40, "modified original DB domain name in DAL"),

    RECORDED_NEW_MHA(50, "recorded new mha in dal"),

    CONFIGURED(55, "configured drc resources like Replicators and Appliers"),

    ESTABLISHED(60, "drc established and all configuration finished");

    private int code;
    private String description;

    EstablishStatusEnum(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public static EstablishStatusEnum getEnumByCode(int code) {
        for(EstablishStatusEnum establishStatusEnum : EstablishStatusEnum.values()) {
            if(establishStatusEnum.getCode() == code) {
                return establishStatusEnum;
            }
        }
        return null;
    }
}
