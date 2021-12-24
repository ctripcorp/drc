package com.ctrip.framework.drc.core.meta.comparator;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public interface ConfigChanged<T extends Enum<T>> {

    T getChangedType();
}
