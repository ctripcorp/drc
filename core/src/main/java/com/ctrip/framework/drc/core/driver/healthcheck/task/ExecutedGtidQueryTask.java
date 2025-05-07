package com.ctrip.framework.drc.core.driver.healthcheck.task;

import com.ctrip.framework.drc.core.driver.binlog.gtid.db.CompositeGtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.GtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.ShowMasterGtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.TransactionTableGtidReader;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/11/29 下午5:24.
 */
public class ExecutedGtidQueryTask extends AbstractQueryTask<String> {

    private CompositeGtidReader gtidReader = new CompositeGtidReader();

    public ExecutedGtidQueryTask(Endpoint master) {
        super(master);
        this.gtidReader.addGtidReader(Lists.newArrayList(new ShowMasterGtidReader(), new TransactionTableGtidReader(master)));
    }

    public ExecutedGtidQueryTask(Endpoint master, List<GtidReader> gtidReaderList) {
        super(master);
        this.gtidReader.addGtidReader(gtidReaderList);
    }

    @Override
    @VisibleForTesting
    public String doQuery() {
        return getExecutedGtid(master);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    protected String getExecutedGtid(Endpoint endpoint) {
        try {
            return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.composite.gtidset.reader", endpoint.getHost() + ":" + endpoint.getPort(), () -> {
                DataSource dataSource = dataSourceManager.getDataSource(endpoint, true);
                try (Connection connection = dataSource.getConnection()){
                    return gtidReader.getExecutedGtids(connection);
                } catch (Exception e) {
                    logger.warn("query executedGtid of {}:{} error", endpoint.getHost(), endpoint.getPort(), e);
                }
                return StringUtils.EMPTY;
            });
        } catch (Exception e) {
            logger.error("query executedGtid of {}:{} error", endpoint.getHost(), endpoint.getPort(), e);
            return StringUtils.EMPTY;
        }
    }
}
