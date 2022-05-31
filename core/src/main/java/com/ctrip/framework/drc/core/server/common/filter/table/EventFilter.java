package com.ctrip.framework.drc.core.server.common.filter.table;

import com.ctrip.framework.drc.core.server.common.filter.table.exception.FilterException;

/**
 * Created by jixinwang on 2021/11/17
 */
public interface EventFilter<T> {

    boolean filter(T event) throws FilterException;
}
