package com.ctrip.framework.drc.core.server.common.filter;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public interface FilterChainFactory<C, V> {

    Filter<V> createFilterChain(C context) throws Exception;
}
