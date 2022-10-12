package com.ctrip.framework.drc.applier.resource.context;

/**
 * Created by jixinwang on 2022/10/12
 */
public enum EventType {

    // Insert row
    INSERT("I"),

    // Update row
    UPDATE("U"),

    // Delete row
    DELETE("D");

    private String value;

    private EventType(String value){
        this.value = value;
    }

    public boolean isInsert() {
        return this.equals(EventType.INSERT);
    }

    public boolean isUpdate() {
        return this.equals(EventType.UPDATE);
    }

    public boolean isDelete() {
        return this.equals(EventType.DELETE);
    }

    public static EventType valuesOf(String value) {
        EventType[] eventTypes = values();
        for (EventType eventType : eventTypes) {
            if (eventType.value.equalsIgnoreCase(value)) {
                return eventType;
            }
        }
        return null;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
