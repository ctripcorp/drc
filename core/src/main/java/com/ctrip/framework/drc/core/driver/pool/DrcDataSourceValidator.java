package com.ctrip.framework.drc.core.driver.pool;

import com.ctrip.framework.drc.core.service.exceptions.InvalidConnectionException;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.apache.tomcat.jdbc.pool.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.IS_READ_ONLY_COMMAND;

/**
 * Created by jixinwang on 2022/7/20
 */
public class DrcDataSourceValidator implements Validator {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private PoolProperties properties;

    private String name;

    private String url;

    public DrcDataSourceValidator(PoolProperties properties) {
        this.properties = properties;
        this.name = properties.getName();
        this.url = properties.getUrl();

        setValidateFlag();
    }

    private void setValidateFlag() {
        properties.setTestOnConnect(true);
        properties.setTestOnBorrow(true);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public boolean validate(Connection connection, int validateAction) {
        boolean isMater = false;
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(IS_READ_ONLY_COMMAND)) {
                if (resultSet.next()) {
                    isMater = "OFF".equalsIgnoreCase(resultSet.getString("Value"));
                    // logger.info("DRC DataSource master validation of connection: {}, with result {}, for name: {}, url: {}", connection, isMater, name, url);
                }
            }
        } catch (Exception e) {
            logger.warn("DRC DataSource master validation error, for name: {}, url: {}", name, url, e);
            return false;
        }

        if (isMater) {
            return true;
        } else {
            logger.error("DRC DataSource master validation false, for name: {}, url: {}", name, url);
            throw new InvalidConnectionException(String.format("Borrowed drc connection is not master, for name: %s, url: %s", name, url));
        }
    }
}
