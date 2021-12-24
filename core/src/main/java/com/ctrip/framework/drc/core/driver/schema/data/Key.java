package com.ctrip.framework.drc.core.driver.schema.data;

/**
 * @Author Slight
 * Oct 12, 2019
 */
public abstract class Key {

    public abstract String toString();

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof Key) {
            return obj.toString().equals(this.toString());
        }
        return false;
    }
}
