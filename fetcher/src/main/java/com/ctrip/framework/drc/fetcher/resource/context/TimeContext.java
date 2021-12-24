package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.TableKey;

public interface TimeContext extends Context.Simple {
    String KEY_NAME = "execute time";

    default void updateExecuteTime(long executeTime) {
        update(KEY_NAME, executeTime);
    }

    default long fetchExecuteTime() {
        return (long) fetch(KEY_NAME);
    }
}
