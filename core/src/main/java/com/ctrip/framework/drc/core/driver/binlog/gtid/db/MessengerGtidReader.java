package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by dengquanliang
 * 2025/1/10 15:27
 */
public class MessengerGtidReader implements GtidReader {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SELECT_MESSENGER_GTID_EXECUTED = "select `gtid_set` from `drcmonitordb`.`messenger_gtid_executed` where `registry_key` = '%s';";


    private Endpoint endpoint;
    private String registryKey;

    public MessengerGtidReader(String registryKey, Endpoint endpoint) {
        this.registryKey = registryKey;
        this.endpoint = endpoint;
    }

    @Override
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public String getExecutedGtids(Connection connection) throws Exception {
        return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.messenger.gtidset.reader.query", registryKey + "-" + endpoint.getHost() + ":" + endpoint.getPort(), () -> {
            String gtidExecuted = "";
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(String.format(SELECT_MESSENGER_GTID_EXECUTED, registryKey))) {

                while (resultSet.next()) {
                    gtidExecuted =  resultSet.getString(1);
                }
            } catch (Exception e) {
                logger.warn("execute select sql error, sql is: {}", SELECT_MESSENGER_GTID_EXECUTED, e);
                throw e;
            }
            return gtidExecuted;
        });
    }

}
