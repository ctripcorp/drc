package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by jixinwang on 2022/1/15
 */
public interface TableKeyMapContext extends Context.Simple {

    String KEY_NAME = "table key map";

    default void resetTableKeyMap() {
        update(KEY_NAME, Maps.newHashMap());
    }

    default void updateTableKeyMap(Long tableId, TableKey tableKey) {
        Map<Long, TableKey> tableKeyMap = (Map<Long, TableKey>) fetch(KEY_NAME);
        tableKeyMap.put(tableId, tableKey);
    }

    default TableKey fetchTableKeyInMap(Long id) {
        Map<Long, TableKey> tableKeyMap = (Map<Long, TableKey>) fetch(KEY_NAME);
        return tableKeyMap.get(id);
    }
}
