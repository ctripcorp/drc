package com.ctrip.framework.drc.core.utils;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MQ;

/**
 * Created by jixinwang on 2023/11/30
 */
public class NameUtils {

    public static String getApplierRegisterKey(String clusterId, Applier applier) {
        String registryKey = clusterId + "." + applier.getTargetMhaName();
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applier.getApplyMode())) {
            registryKey = registryKey + "." + applier.getIncludedDbs();
        }
        return registryKey;
    }

    public static String getApplierBackupRegisterKey(Applier applier) {
        String registryKey = RegistryKey.from(applier.getTargetName(), applier.getTargetMhaName());
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applier.getApplyMode())) {
            registryKey = registryKey + "." + applier.getIncludedDbs();
        }
        return registryKey;
    }

    public static String getMessengerRegisterKey(String clusterId, Messenger messenger) {
        String registryKey = clusterId + "." + DRC_MQ;
        if (ApplyMode.db_mq == ApplyMode.getApplyMode(messenger.getApplyMode())) {
            registryKey = registryKey + "." + messenger.getIncludedDbs();
        }
        return registryKey;
    }

    public static String getMessengerDbName(Messenger messenger) {
        return ApplyMode.db_mq == ApplyMode.getApplyMode(messenger.getApplyMode()) ? messenger.getIncludedDbs() : DRC_MQ;
    }

    public static String getMessengerDbName(String registryKey) {
        String[] split = registryKey.split("\\.");
        if (split.length < 3) {
            return null;
        }
        return split[split.length - 1];
    }

    public static String dotSchemaIfNeed(String name, int applyMode, String includedDbs) {
        String finalName = name;
        switch (ApplyMode.getApplyMode(applyMode)) {
            case db_transaction_table:
            case db_mq:
                finalName = name + "." + includedDbs;
                break;
            default:
        }
        return finalName;
    }
}
