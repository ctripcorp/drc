package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

/**
 * Created by jixinwang on 2021/9/15
 */
public class CompositeGtidReader implements GtidReader {

    private List<GtidReader> gtidReaderList = Lists.newArrayList();

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public String getExecutedGtids(Connection connection) {
        GtidSet gtidSet = new GtidSet("");
        for (GtidReader gtidReader : gtidReaderList) {
            try {
                String executedGtid = gtidReader.getExecutedGtids(connection);
                gtidSet = gtidSet.union(new GtidSet(executedGtid));
            } catch (Exception e) {
                logger.error("CompositeGtidReader get executed gtidset error", e);
                gtidSet = new GtidSet("");
            }
        }
        return gtidSet.toString();
    }

    public synchronized void addGtidReader(List<GtidReader> gtidReaders) {
        for (GtidReader gtidReader : gtidReaders) {
            if (!gtidReaderList.contains(gtidReader)) {
                gtidReaderList.add(gtidReader);
            }
        }
    }
}
