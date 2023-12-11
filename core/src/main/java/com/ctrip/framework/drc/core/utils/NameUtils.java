package com.ctrip.framework.drc.core.utils;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;

/**
 * Created by jixinwang on 2023/11/30
 */
public class NameUtils {

    public static String getApplierRegisterKey(String clusterId, Applier applier) {
        String registryKey = clusterId + "." + applier.getTargetMhaName();
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applier.getApplyMode())) {
            String dbName = applier.getIncludedDbs();
            registryKey = registryKey + "." + dbName;
        }
        return registryKey;
    }

    public static String dotSchemaIfNeed(String name, Applier applier) {
        String finalName = name;
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applier.getApplyMode())) {
            String dbName = applier.getIncludedDbs();
            finalName = name + "." + dbName;
        }
        return finalName;
    }

    public static String dotSchemaIfNeed(String name, int applyMode, String includedDbs) {
        String finalName = name;
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applyMode)) {
            finalName = name + "." + includedDbs;
        }
        return finalName;
    }
}
