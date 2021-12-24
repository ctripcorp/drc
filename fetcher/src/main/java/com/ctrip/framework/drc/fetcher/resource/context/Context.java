package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.fetcher.system.Resource;

/**
 * @Author Slight
 * Oct 13, 2019
 */
public interface Context extends Resource, Resource.Dynamic {

    //Basically a Context must be a resource which has a lifecycle.
    //Here I delete 'extends Resource' for exchange of convenience of unit test.

    //Possibly it is a good idea to add default methods to Resource and still mark
    //Context as a Resource, I just don't try.

    interface Simple extends Context {
        void update(String key, Object value);
        Object fetch(String key);
    }
}
