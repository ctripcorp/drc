package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.TableKey;

/**
 * @Author Slight
 * Oct 13, 2019
 * <p>
 * removed in the future.
 */
public interface TableKeyContext extends Context.Simple {

    String KEY_NAME = "table key";

    default void updateTableKey(TableKey tableKey) {
        update(KEY_NAME, tableKey);
    }

    default TableKey fetchTableKey() {
        return (TableKey) fetch(KEY_NAME);
    }
}
