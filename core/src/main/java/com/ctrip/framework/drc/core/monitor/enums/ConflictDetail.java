package com.ctrip.framework.drc.core.monitor.enums;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.ctrip.framework.drc.core.monitor.enums.ConflictDetail.AlertLevel.*;
import static com.ctrip.framework.drc.core.monitor.enums.ConflictResult.COMMIT;
import static com.ctrip.framework.drc.core.monitor.enums.ConflictResult.ROLLBACK;
import static com.ctrip.framework.drc.core.monitor.enums.DmlEnum.*;

public enum ConflictDetail {
    /**
     * commit
     */
    // insert
    INSERT_UNKNOWN_COLUMN(COMMIT, INSERT, INFO),
    INSERT_TO_UPDATE(COMMIT, INSERT, WARN),
    // update
    UPDATE_UNKNOWN_COLUMN(COMMIT, UPDATE, INFO),
    UPDATE_NO_PARAMETER(COMMIT, UPDATE, INFO),
    UPDATE_OLD_TO_NEW(COMMIT, UPDATE, WARN),
    UPDATE_TO_INSERT(COMMIT, UPDATE, WARN),
    // delete
    DELETE_NOT_FOUND(COMMIT, DELETE, INFO),
    DELETE_NO_PARAMETER(COMMIT, DELETE, INFO),

    /**
     * rollback
     */
    // insert
    INSERT_TO_UPDATE_SAME_EXIST(ROLLBACK, INSERT, INFO),
    INSERT_TO_UPDATE_NEWER_EXIST(ROLLBACK, INSERT, CRITICAL),
    // update
    UPDATE_SAME_EXIST(ROLLBACK, UPDATE, INFO),
    UPDATE_NEWER_EXIST(ROLLBACK, UPDATE, CRITICAL),
    UPDATE_TO_INSERT_DUPLICATE_KEY(ROLLBACK, UPDATE, CRITICAL), // UNLIKELY

    // unknown exception
    INSERT_EXCEPTION(ROLLBACK, INSERT, CRITICAL),
    UPDATE_EXCEPTION(ROLLBACK, UPDATE, CRITICAL),
    DELETE_EXCEPTION(ROLLBACK, DELETE, CRITICAL),
    ;

    private final ConflictResult conflictResult;
    private final DmlEnum dmlEnum;
    private final AlertLevel alertLevel;

    ConflictDetail(ConflictResult conflictResult, DmlEnum dmlEnum, AlertLevel alertLevel) {
        this.conflictResult = conflictResult;
        this.dmlEnum = dmlEnum;
        this.alertLevel = alertLevel;
    }

    public ConflictResult getConflictResult() {
        return conflictResult;
    }

    private static final Map<String, ConflictDetail> NAME_TO_VALUE_MAP;

    static {
        Map<String, ConflictDetail> map = new HashMap<>();
        for (ConflictDetail value : values()) {
            map.put(value.name(), value);
        }
        NAME_TO_VALUE_MAP = Collections.unmodifiableMap(map);
    }

    public DmlEnum getDmlEnum() {
        return dmlEnum;
    }

    public AlertLevel getAlertLevel() {
        return alertLevel;
    }

    public static ConflictResult getConflictResult(String conflictDetail) {
        return Optional.ofNullable(NAME_TO_VALUE_MAP.get(conflictDetail))
                .map(ConflictDetail::getConflictResult)
                .orElse(ROLLBACK);
    }

    public enum AlertLevel {
        /**
         * no need to inform user
         */
        INFO,
        /**
         * data is consistent after commit,
         */
        WARN,
        CRITICAL,
    }
}
