package com.ctrip.framework.drc.core.entity;

/**
 * Created by dengquanliang
 * 2024/7/30 14:31
 */
public interface CloneableInstance<T> extends Cloneable {
    T cloneInstance() throws CloneNotSupportedException;
}
