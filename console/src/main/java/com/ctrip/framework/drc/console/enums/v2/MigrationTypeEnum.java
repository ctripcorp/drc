package com.ctrip.framework.drc.console.enums.v2;

import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;

/**
 * Created by shiruixin
 * 2025/4/29 16:49
 */
public enum MigrationTypeEnum {
    COMMON_INIT(MainTypeEnum.COMMON, MigrationStatusEnum.INIT, null),
    COMMON_PRESTART(MainTypeEnum.COMMON, MigrationStatusEnum.PRE_STARTING, MigrationStatusEnum.INIT),
    COMMON_START(MainTypeEnum.COMMON, MigrationStatusEnum.STARTING, MigrationStatusEnum.PRE_STARTED),
    COMMON_CHECK_NEW_MHA(MainTypeEnum.COMMON, MigrationStatusEnum.READY_TO_SWITCH_DAL, MigrationStatusEnum.STARTING),
    COMMON_CHECK_OLD_MHA(MainTypeEnum.COMMON, MigrationStatusEnum.READY_TO_COMMIT_TASK, MigrationStatusEnum.READY_TO_SWITCH_DAL),

    OVERSEA_INIT(MainTypeEnum.OVERSEA, MigrationStatusEnum.INIT, null),
    OVERSEA_PRESTART(MainTypeEnum.OVERSEA, MigrationStatusEnum.PRE_STARTING,  MigrationStatusEnum.INIT),
    OVERSEA_START_SHA_TO_OVERSEA(MainTypeEnum.OVERSEA, MigrationStatusEnum.STARTING_SHA_TO_OVERSEA, MigrationStatusEnum.PRE_STARTED),
    OVERSEA_CHECK_SHA_TO_OVERSEA(MainTypeEnum.OVERSEA, MigrationStatusEnum.READY_TO_DISCONNECT_DB_SYNC, MigrationStatusEnum.STARTING_SHA_TO_OVERSEA),
    OVERSEA_START_OVERSEA_TO_SHA(MainTypeEnum.OVERSEA, MigrationStatusEnum.STARTING_OVERSEA_TO_SHA, MigrationStatusEnum.READY_TO_DISCONNECT_DB_SYNC),
    OVERSEA_CHECK_OVERSEA_TO_SHA(MainTypeEnum.OVERSEA, MigrationStatusEnum.READY_TO_COMMIT_TASK, MigrationStatusEnum.STARTING_OVERSEA_TO_SHA),
    ;

    enum MainTypeEnum {
        COMMON,
        OVERSEA
    }

    private MainTypeEnum mainType;
    private MigrationStatusEnum successStatus;
    private MigrationStatusEnum curValidStatus;

    MigrationTypeEnum(MainTypeEnum mainType, MigrationStatusEnum successStatus, MigrationStatusEnum curValidStatus) {
        this.mainType = mainType;
        this.successStatus = successStatus;
        this.curValidStatus = curValidStatus;
    }

    public MainTypeEnum getMainType() {
        return mainType;
    }

    public void setMainType(MainTypeEnum mainType) {
        this.mainType = mainType;
    }

    public MigrationStatusEnum getSuccessStatus() {
        return successStatus;
    }

    public void setSuccessStatus(MigrationStatusEnum successStatus) {
        this.successStatus = successStatus;
    }

    public MigrationStatusEnum getCurValidStatus() {
        return curValidStatus;
    }

    public void setCurValidStatus(MigrationStatusEnum curValidStatus) {
        this.curValidStatus = curValidStatus;
    }

    public boolean isCommon() {
        return mainType == MainTypeEnum.COMMON;
    }
}
