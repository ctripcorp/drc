package com.ctrip.framework.drc.console.enums.operation;

/**
 * @ClassName OperationType
 * @Author haodongPan
 * @Date 2023/12/11 14:31
 * @Version: $
 */
public enum OperateTypeEnum {
    MHA_REPLICATION("MHA同步","MHA_replication"),
    MESSENGER_REPLICATION("Messenger同步","messenger_replication"),

    DB_MIGRATION("DB迁移","db_migration"),
    CONFLICT_RESOLUTION("冲突处理","conflict_resolution");

    private String name;
    private String val;

    OperateTypeEnum(String name, String val) {
        this.name = name;
        this.val = val;
    }

    public String getName() {
        return name;
    }

    public String getVal() {
        return val;
    }

}
