package com.ctrip.framework.drc.core.utils;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;

import java.util.Arrays;

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

    public static String getApplierRegisterKey(String clusterId, String applierBackupRegistryKey) {
        String[] split = applierBackupRegistryKey.split("\\.");
        StringBuilder sb = new StringBuilder();
        sb.append(clusterId);
        for (int i = 1; i < split.length; i++) {
            sb.append(".").append(split[i]);
        }
        return sb.toString();
    }

    public static String getApplierBackupRegisterKey(Applier applier) {
        String registryKey = RegistryKey.from(applier.getTargetName(), applier.getTargetMhaName());
        if (ApplyMode.db_transaction_table == ApplyMode.getApplyMode(applier.getApplyMode())) {
            registryKey = registryKey + "." + applier.getIncludedDbs();
        }
        return registryKey;
    }

    public static String getMessengerRegisterKey(String clusterId, Messenger messenger) {
        ApplyMode applyMode = ApplyMode.getApplyMode(messenger.getApplyMode());
        MqType mqType = MqType.parseByApplyMode(applyMode);
        String registryKey = clusterId + "." + mqType.getRegistryKeySuffix();
        if (applyMode.isDbGranular()) {
            registryKey = registryKey + "." + messenger.getIncludedDbs();
        }
        return registryKey;
    }

    /**
     * format:  registryKey -> messengerLogicDbName
     * <p>
     * mha messenger: mha_dalcluster.mha.[suffix] -> [suffix]
     * <p>
     * db messenger: mha_dalcluster.mha.[suffix].[dbName] -> [suffix].[dbName]
     */
    public static String getMessengerRegisterKey(String clusterId, String messengerLogicDbName) {
        return clusterId + '.' + messengerLogicDbName;
    }

    public static String getMessengerDbName(Messenger messenger) {
        ApplyMode applyMode = ApplyMode.getApplyMode(messenger.getApplyMode());
        MqType mqType = MqType.parseByApplyMode(applyMode);
        String messengerLogicDbName = mqType.getRegistryKeySuffix();
        if (applyMode.isDbGranular()) {
            messengerLogicDbName += "." + messenger.getIncludedDbs();
        }
        return messengerLogicDbName;
    }

    public static String getMessengerDbName(String registryKey) {
        String[] split = registryKey.split("\\.");
        if (split.length < 3) {
            return null;
        }
        return String.join(".", Arrays.copyOfRange(split, 2, split.length));
    }

    public static ApplyMode getMessengerApplyMode(String registryKey) {
        String[] split = registryKey.split("\\.");
        MqType mqType = MqType.parseBySuffix(split[2]);
        if (split.length == 3) {
            return mqType.getMhaApplyMode();
        } else {
            return mqType.getDbApplyMode();
        }
    }
    public static MqType getMessengerMqType(String registryKey) {
        String[] split = registryKey.split("\\.");
        return MqType.parseBySuffix(split[2]);
    }

    public static String dotSchemaIfNeed(String name, int applyMode, String includedDbs) {
        String finalName = name;
        if (ApplyMode.getApplyMode(applyMode).isDbGranular()) {
            finalName = name + "." + includedDbs;
        }
        return finalName;
    }
}
