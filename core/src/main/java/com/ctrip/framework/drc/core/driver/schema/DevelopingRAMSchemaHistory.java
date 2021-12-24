package com.ctrip.framework.drc.core.driver.schema;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.Schemas;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author Slight
 * Mar 20, 2020
 */
public class DevelopingRAMSchemaHistory implements SchemasHistory {

    private Logger logger = LoggerFactory.getLogger(getClass());

    List<GtidSet> gtidSetSequence = Lists.newArrayList();
    List<Schemas> schemaSequence = Lists.newArrayList();
    GtidSet lastGtidSet = new GtidSet("");
    Schemas lastSchemas = Schemas.empty();

    @Override
    public void merge(GtidSet gtidExecuted, TableKey tableKey, Columns columns) throws Exception {
        if (gtidExecuted.equals(lastGtidSet)) {
            lastSchemas.put(tableKey, columns);
        } else if (lastGtidSet.isContainedWithin(gtidExecuted)) {
            Schemas newSchemas = Schemas.empty();
            newSchemas.put(tableKey, columns);
            gtidSetSequence.add(gtidExecuted);
            schemaSequence.add(newSchemas);
            lastGtidSet = gtidExecuted;
        } else {
            logger.info("[lastGtidSet] is {}, [gtidExecuted] is {}", lastGtidSet, gtidExecuted);
            throw new Exception("invalid gtidExecuted");
        }
    }

    @Override
    public boolean isMerged(GtidSet gtidExecuted, TableKey tableKey, Columns columns) {
        int i = gtidSetSequence.indexOf(gtidExecuted);
        if (i == -1) {
            return false;
        }
        Schemas schemes = schemaSequence.get(i);
        if (!schemes.containsKey(tableKey)) {
            return false;
        }
        if (!schemes.getColumns(tableKey.getDatabaseName(), tableKey.getTableName()).equals(columns)) {
            return false;
        }
        return true;
    }

    @Override
    public Schemas query(String gtid) {
        logger.info("history.query() - gtid: {} last gtid set: {}, gtid set sequence: {}",
                gtid, lastGtidSet, gtidSetSequence);
        if (new GtidSet(gtid).isContainedWithin(lastGtidSet)) {
            for (int i = 0; i < gtidSetSequence.size(); i++) {
                if (new GtidSet(gtid).isContainedWithin(gtidSetSequence.get(i))) {
                    if (i == 0) {
                        return null;
                    } else {
                        return schemaSequence.get(i - 1);
                    }
                }
            }
        } else {
            return lastSchemas;
        }
        return null;
    }
}
