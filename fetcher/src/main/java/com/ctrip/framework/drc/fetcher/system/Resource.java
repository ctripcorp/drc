package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Initializable;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public interface Resource extends Unit, Initializable, Disposable {

    //Dynamic Resource: those whose lifecycle are not managed directly by a system.
    interface Dynamic extends Resource {

        //dispose() of resource must not fail,
        // or the failure can be ignore.
        default void mustDispose() {
            try {
                dispose();
            } catch (Throwable t) {

            }
        }
    }
}


