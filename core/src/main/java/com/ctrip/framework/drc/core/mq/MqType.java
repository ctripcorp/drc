package com.ctrip.framework.drc.core.mq;

import com.ctrip.framework.drc.core.meta.ReplicationTypeEnum;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;

import java.util.Arrays;
import java.util.function.Function;

/**
 * Created by jixinwang on 2022/10/17
 */
public enum MqType {
    qmq(ApplyMode.mq, ApplyMode.db_mq, ReplicationTypeEnum.DB_TO_MQ, SystemConfig.DRC_MQ),
    kafka(ApplyMode.kafka, null, ReplicationTypeEnum.DB_TO_KAFKA, SystemConfig.DRC_KAFKA),
    ;


    private final ApplyMode mhaApplyMode;
    private final ApplyMode dbApplyMode;
    private final ReplicationTypeEnum replicationType;
    private final String registryKeySuffix;

    public static final MqType DEFAULT = qmq;

    MqType(ApplyMode mhaApplyMode, ApplyMode dbApplyMode, ReplicationTypeEnum replicationType, String registryKeySuffix) {
        this.mhaApplyMode = mhaApplyMode;
        this.dbApplyMode = dbApplyMode;
        this.replicationType = replicationType;
        this.registryKeySuffix = registryKeySuffix;
    }

    public ReplicationTypeEnum getReplicationType() {
        return replicationType;
    }


    public String getRegistryKeySuffix() {
        return registryKeySuffix;
    }

    public static MqType parseBySuffix(String suffix) {
        return parseByAttribute(suffix, MqType::getRegistryKeySuffix);
    }

    public static MqType parseByReplicationType(ReplicationTypeEnum replicationTypeEnum) {
        return parseByAttribute(replicationTypeEnum, MqType::getReplicationType);
    }

    public static MqType parseOrDefault(String name) {
        MqType res = parseByAttribute(name, MqType::name);
        return res == null ? DEFAULT : res;
    }
    public static MqType parse(String name) {
        return parseByAttribute(name, MqType::name);
    }

    private static <T> MqType parseByAttribute(T attribute, Function<MqType, T> attributeGetter) {
        if (attribute == null) {
            return null;
        }
        for (MqType mqType : MqType.values()) {
            if (attribute.equals(attributeGetter.apply(mqType))) {
                return mqType;
            }
        }
        return null;
    }

    public ApplyMode getMhaApplyMode() {
        return mhaApplyMode;
    }

    public ApplyMode getDbApplyMode() {
        return dbApplyMode;
    }

    public static MqType parseByApplyMode(ApplyMode applyMode) {
        for (MqType mqType : MqType.values()) {
            if (mqType.mhaApplyMode == applyMode || mqType.dbApplyMode == applyMode) {
                return mqType;
            }
        }
        throw new IllegalStateException("not supported applymode: " + applyMode);
    }

    /**
     * if support dal client, should update binlog-topic-registry as well
     */
    public boolean notSupportDalClient() {
        return this != qmq;
    }

    public static boolean isMessengerRegistryKeySuffix(String str) {
        return Arrays.stream(MqType.values()).anyMatch(mqType -> mqType.getRegistryKeySuffix().equals(str));
    }
}