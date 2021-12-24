package com.ctrip.framework.drc.core.driver.schema;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.Schemas;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;

/**
 * @Author Slight
 * Oct 12, 2019
 */
public class FixedRAMSchemasHistory implements SchemasHistory {

    protected Schemas fixedSchema = Schemas.empty();

    @Override
    public void merge(GtidSet gtidExecuted, TableKey tableKey, Columns columns) {
        fixedSchema = fixedSchema.merge(tableKey, columns);
    }

    @Override
    public boolean isMerged(GtidSet gtidExecuted, TableKey tableKey, Columns columns) {
        if (!fixedSchema.containsKey(tableKey)) {
            return false;
        }
        if (!fixedSchema.getColumns(tableKey.getDatabaseName(), tableKey.getTableName()).equals(columns)) {
            return false;
        }
        return true;
    }

    @Override
    public Schemas query(String gtid) {
        return fixedSchema;
    }
}
