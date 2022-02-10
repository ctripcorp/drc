package com.ctrip.framework.drc.console.monitor.healthcheck.task;

import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.CompositeGtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.ShowMasterGtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.TransactionTableGtidReader;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-25
 */
public class ExecutedGtidQueryTask extends AbstractQueryTask<String> {

    protected static final String ALI_RDS = "/*FORCE_MASTER*/";

    private static final String EXECUTED_GTID_SUPER = ALI_RDS + "show master status;";

    public ExecutedGtidQueryTask(Endpoint master) {
        super(master);
        this.gtidReader.addGtidReader(Lists.newArrayList(new ShowMasterGtidReader(), new TransactionTableGtidReader()));
    }

    private CompositeGtidReader gtidReader = new CompositeGtidReader();
    
    //    @Override
//    protected String doQuery() {
//        return MySqlUtils.getExecutedGtid(master);
//    }
    @Override
    protected String doQuery() {
        return getExecutedGtid(master);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    protected String getExecutedGtid(Endpoint endpoint) {
        DataSource dataSource = dataSourceManager.getDataSource(endpoint);
        try (Connection connection = dataSource.getConnection()) {
            return gtidReader.getExecutedGtids(connection);
        } catch (Exception e) {
            logger.warn("query executedGtid({}) of {}:{} error", getCommand(), endpoint.getHost(), endpoint.getPort(), e);
        }
        return StringUtils.EMPTY;
    }

    public String getCommand() {
        return EXECUTED_GTID_SUPER;
    }
}
