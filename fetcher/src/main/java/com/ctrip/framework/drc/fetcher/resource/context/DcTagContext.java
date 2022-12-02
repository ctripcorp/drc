package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.mq.DcTag;

/**
 * Created by jixinwang on 2022/10/19
 */
public interface DcTagContext extends Context.Simple, Context {

    String KEY_NAME = "dcTag";

    String KEY_RESET = null;

    default void updateDcTag(DcTag dcTag) {
        update(KEY_NAME, dcTag);
    }

    default void resetDcTag() {
        update(KEY_NAME, KEY_RESET);
    }

    default DcTag fetchDcTag() {
        return (DcTag) fetch(KEY_NAME);
    }
}
