package com.ctrip.framework.drc.core.driver.config;

/**
 * Created by mingdongli
 * 2019/10/30 上午10:40.
 */
public enum InstanceStatus {

    ACTIVE(0),   //master

    INACTIVE(1), //slave

    DELETED(2);

    private int status;

    InstanceStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static InstanceStatus getInstanceStatus(int status) {
        for (InstanceStatus instanceStatus : values()) {
            if (instanceStatus.getStatus() == status) {
                return instanceStatus;
            }
        }

        throw new UnsupportedOperationException("not support for status " + status);
    }
}
