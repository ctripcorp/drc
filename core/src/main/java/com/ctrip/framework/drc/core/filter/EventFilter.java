package com.ctrip.framework.drc.core.filter;

import com.ctrip.framework.drc.core.filter.exception.FilterException;

/**
 * Created by jixinwang on 2021/11/17
 */
public interface EventFilter<T> {

    boolean filter(T event) throws FilterException;
}
