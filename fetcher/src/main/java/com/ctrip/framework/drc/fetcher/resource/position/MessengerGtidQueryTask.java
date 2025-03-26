package com.ctrip.framework.drc.fetcher.resource.position;

import com.ctrip.framework.drc.core.driver.binlog.gtid.db.MessengerGtidReader;
import com.ctrip.framework.drc.core.driver.healthcheck.task.AbstractQueryTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;

/**
 * Created by dengquanliang
 * 2025/1/10 15:51
 */
public class MessengerGtidQueryTask extends AbstractQueryTask<String> {

    private String registryKey;

    public MessengerGtidQueryTask(Endpoint master, String registryKey) {
        super(master);
        this.registryKey = registryKey;
    }

    @Override
    @VisibleForTesting
    public String doQuery() {
        return getExecutedGtid().getLeft();
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Pair<String, Boolean> getExecutedGtid() {
        try {
            return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.messenger.gtidset.reader", master.getHost() + ":" + master.getPort(), () -> {
                DataSource dataSource = dataSourceManager.getDataSource(master);
                try (Connection connection = dataSource.getConnection()){
                    return Pair.of(new MessengerGtidReader(registryKey, master).getExecutedGtids(connection), true);
                } catch (Exception e) {
                    logger.warn("query messenger executedGtid of {}:{} error", master.getHost(), master.getPort(), e);
                    return Pair.of(StringUtils.EMPTY, false);
                }
            });
        } catch (Exception e) {
            logger.error("query messenger executedGtid of {}:{} error", master.getHost(), master.getPort(), e);
            return Pair.of(StringUtils.EMPTY, false);
        }
    }
}
