package com.ctrip.framework.drc.core.driver.binlog.gtid.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName PurgedGtidReader
 * @Author haodongPan
 * @Date 2023/6/2 15:44
 * @Version: $
 */
public class PurgedGtidReader implements GtidReader {
    
    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected static final String ALI_RDS = "/*FORCE_MASTER*/";

    private static final String PURGED_GTID = ALI_RDS + "show global variables like \"gtid_purged\";";

    private static final int PURGED_GTID_INDEX = 2;

    @Override
    public String getExecutedGtids(Connection connection) {
        return select(connection, PURGED_GTID, PURGED_GTID_INDEX);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private String select(Connection connection, String sql, int index) {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                if (resultSet.next()) {
                    return resultSet.getString(index);
                }
            }
        } catch (SQLException e) {
            logger.warn("execute select sql error, sql is: {}", sql, e);
        }
        return StringUtils.EMPTY;
    }
}
