package com.ctrip.framework.drc.core.driver.config;

import com.ctrip.xpipe.api.lifecycle.Ordered;

import java.lang.reflect.Field;

/**
 * @Author Slight
 * Jun 16, 2020
 */
public interface ConfigSource<T> extends Ordered {

    Object get(T key);

    void load(Object object, Field field, T key);

    void bind(Object object, Field field, T key);
}
