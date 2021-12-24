package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.schema.SchemasHistory;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;

/**
 * @Author Slight
 * Oct 13, 2019
 */
public interface SchemasHistoryContext extends GtidSetContext, Context.Simple {

    SchemasHistory getSchemasHistory();

    default void updateSchema(TableKey tableKey, Columns columns) throws Exception {
        getSchemasHistory().merge(fetchGtidSet(), tableKey, columns);
    }

    default Columns fetchColumns(TableKey tableKey) {
        return getSchemasHistory().query(fetchGtid()).get(tableKey);
    }
}
