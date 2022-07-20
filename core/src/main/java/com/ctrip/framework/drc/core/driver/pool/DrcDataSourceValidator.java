package com.ctrip.framework.drc.core.driver.pool;

import org.apache.tomcat.jdbc.pool.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by jixinwang on 2022/7/20
 */
public class DrcDataSourceValidator implements Validator {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final String IS_READ_ONLY_COMMAND = "/*FORCE_MASTER*/show global variables like \"read_only\";";

    @Override
    public boolean validate(Connection connection, int validateAction) {
        boolean isMater = false;
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(IS_READ_ONLY_COMMAND)) {
                if (resultSet.next()) {
                    isMater = "OFF".equalsIgnoreCase(resultSet.getString("Value"));
                    logger.info("DRC DataSource master validation of connection: {}, with result {}", connection, isMater);
                }
            }
        } catch (Exception e) {
            logger.warn("DRC DataSource master validation error", e);
        }

        if (isMater) {
            return true;
        } else {
            throw new RuntimeException("Borrowed drc connection is not master");
        }
    }
}
