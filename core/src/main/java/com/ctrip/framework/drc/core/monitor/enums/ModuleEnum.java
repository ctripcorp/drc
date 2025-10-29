package com.ctrip.framework.drc.core.monitor.enums;

import com.ctrip.framework.drc.core.mq.MqType;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-15
 */
public enum ModuleEnum {

    REPLICATOR(0, "R", 100023498L),

    APPLIER(1, "A", 100023500L),

    CLUSTER_MANAGER(2, "CM", 100025243L),

    CONSOLE(3, "C", 100023928L),

    ZOOKEEPER(4, "Z", 100023934L),

    VALIDATION(5, "V", 100030428L),

    PROXY(6, "P", 100013684L),

    MESSENGER_QMQ(7, "MQ", 100059182L),

    MESSENGER_KAFKA(8, "MK", 100059182L);

    private int code;

    private String description;

    private long appId;

    ModuleEnum(int code, String description, long appId) {
        this.code = code;
        this.description = description;
        this.appId = appId;
    }


    public static boolean isMessenger(int code) {
        return code == MESSENGER_QMQ.code || code == MESSENGER_KAFKA.code;
    }

    public static boolean isResource(int code) {
        return code == REPLICATOR.code || code == APPLIER.code || code == MESSENGER_QMQ.code || code == MESSENGER_KAFKA.code;
    }

    public static int getMessengerCodeByMqType(MqType mqType) {
        return switch (mqType) {
            case qmq -> MESSENGER_QMQ.code;
            case kafka -> MESSENGER_KAFKA.code;
        };
    }

    public static ModuleEnum getModuleEnum(String description) throws Exception {
        for (ModuleEnum moduleEnum : ModuleEnum.values()) {
            if (description.equals(moduleEnum.getDescription())) {
                return moduleEnum;
            }
        }
        throw new Exception("wrong description: " + description);
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public long getAppId() {
        return appId;
    }
}
