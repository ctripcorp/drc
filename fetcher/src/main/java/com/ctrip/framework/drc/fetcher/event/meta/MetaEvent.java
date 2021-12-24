package com.ctrip.framework.drc.fetcher.event.meta;

import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.fetcher.resource.context.Context;

/**
 * @Author Slight
 * Sep 30, 2019
 */
public interface MetaEvent<T extends Context> extends FetcherEvent {

    void involve(T context) throws Exception;

    interface Write<T extends Context> extends MetaEvent<T> {
    }

    interface Read<T extends Context> extends MetaEvent<T> {
    }
}
