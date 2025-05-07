package com.ctrip.framework.drc.core.monitor.enums;

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

    MESSENGER(7, "M", 100059182L);

    private int code;

    private String description;

    private long appId;

    ModuleEnum(int code, String description, long appId) {
        this.code = code;
        this.description = description;
        this.appId = appId;
    }

    public static ModuleEnum getModuleEnum(String description) throws Exception {
        for(ModuleEnum moduleEnum: ModuleEnum.values()){
            if(description.equals(moduleEnum.getDescription())){
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
