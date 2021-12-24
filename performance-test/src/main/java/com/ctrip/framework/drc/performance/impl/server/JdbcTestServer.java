package com.ctrip.framework.drc.performance.impl.server;

import com.ctrip.framework.drc.applier.server.ApplierServerInCluster;
import com.ctrip.framework.drc.core.driver.pool.DrcTomcatDataSource;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.performance.utils.ConfigService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.framework.drc.core.monitor.datasource.AbstractDataSource.setCommonProperty;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;

/**
 * Created by jixinwang on 2021/9/9
 */
public class JdbcTestServer extends ApplierServerInCluster {

    private PoolProperties properties;

    private javax.sql.DataSource inner;

    public int validationInterval = 30000;

    protected ExecutorService internal;

    public int threadNum = ConfigService.getInstance().getJdbcThreadNumber();

    public int executeCount = 200;

    private AtomicInteger count = new AtomicInteger(0);

    private long last = 0;

    private long current = 0;

    private static final String INSERT1k = "insert into `bbzbbzdrcbenchmarktmpdb`.`benchmark1` (`charlt256`, `chareq256`, `varcharlt256`, `varchargt256`, `datachange_lasttime`, `drc_id_int`, `addcol`, `addcol1`, `addcol2`, `drc_char_test_2`, `drc_tinyint_test_2`, `drc_bigint_test`, `drc_integer_test`, `drc_mediumint_test`, `drc_time6_test`, `drc_datetime3_test`, `drc_year_test`, `hourly_rate_3`, `drc_numeric10_4_test`, `drc_float_test`, `drc_double_test`, `drc_double10_4_test`, `drc_real_test`, `drc_real10_4_test`) values\n" +
            "('China1', 'yuかな1', 'присоска1', 'abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ababc11abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abbcbc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ac1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1c1abc1abc1abc1abc1abc1abc1abc1abc1abc1a', NOW(), 123, 'addcol1', 'addcol2', 'addcol3', 'charvalue', 11, 22, 33, 44, '02:12:22', '2020-04-03 11:11:03', '2008', 1.04, 10.0, 13, 1234, 13.1212, 345, 12.3);";

    private ScheduledExecutorService scheduledExecutorService;

    private static final String CREATE_DB = "CREATE database IF NOT EXISTS bbzbbzdrcbenchmarktmpdb;";

    private static final String CREATE_TABLE = "CREATE TABLE `bbzbbzdrcbenchmarktmpdb`.`benchmark1` (\n" +
            "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '',\n" +
            "  `charlt256` char(30) DEFAULT NULL COMMENT '',\n" +
            "  `chareq256` char(128) DEFAULT NULL COMMENT '',\n" +
            "  `chargt256` char(255) DEFAULT NULL COMMENT '',\n" +
            "  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '',\n" +
            "  `varchareq256` varchar(256) DEFAULT NULL COMMENT '',\n" +
            "  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '',\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'update time',\n" +
            "  `drc_id_int` int(11) NOT NULL DEFAULT '1' COMMENT '',\n" +
            "  `addcol1` varchar(64) DEFAULT 'default_addcol1' COMMENT 'test',\n" +
            "  `addcol2` varchar(64) DEFAULT 'default_addcol2' COMMENT 'test',\n" +
            "  `drc_char_test_2` char(30) DEFAULT 'char' COMMENT '',\n" +
            "  `drc_tinyint_test_2` tinyint(4) DEFAULT '12' COMMENT '',\n" +
            "  `drc_bigint_test` bigint(20) DEFAULT '120' COMMENT '',\n" +
            "  `drc_integer_test` int(11) DEFAULT '11' COMMENT '',\n" +
            "  `drc_mediumint_test` mediumint(9) DEFAULT '12345' COMMENT '',\n" +
            "  `drc_time6_test` time DEFAULT '02:02:02' COMMENT '',\n" +
            "  `drc_datetime3_test` datetime(3) DEFAULT '2019-01-01 01:01:01.000' COMMENT '',\n" +
            "  `drc_year_test` year(4) DEFAULT '2020' COMMENT '',\n" +
            "  `hourly_rate_3` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT '',\n" +
            "  `drc_numeric10_4_test` decimal(10,4) DEFAULT '100.0000' COMMENT '',\n" +
            "  `drc_float_test` float DEFAULT '12' COMMENT '',\n" +
            "  `drc_double_test` double DEFAULT '123' COMMENT '',\n" +
            "  `drc_bit4_test` bit(4) DEFAULT b'11' COMMENT 'TEST',\n" +
            "  `drc_double10_4_test` double(10,4) DEFAULT '123.1245' COMMENT '',\n" +
            "  `drc_real_test` double DEFAULT '234' COMMENT '',\n" +
            "  `drc_real10_4_test` double(10,4) DEFAULT '23.4000' COMMENT '',\n" +
            "  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0' COMMENT '',\n" +
            "  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '',\n" +
            "  `addcol` varchar(50) DEFAULT 'addColName' COMMENT 'add common Name',\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';";

    public JdbcTestServer(ApplierConfigDto config) throws Exception {
        super(config);
    }

    public void define() throws Exception {
    }

    @VisibleForTesting
    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    @VisibleForTesting
    public void setExecuteCount(int executeCount) {
        this.executeCount = executeCount;
    }

    @Override
    public void doInitialize() throws Exception {
        int poolSize = threadNum;
        int maxThreads = threadNum;
        int coreThreads = threadNum;
        logger.info("jdbc thread number is: {}", threadNum);

        executeCount = ConfigService.getInstance().getJdbcExecuteCount();
        logger.info("jdbc execute count is: {}", executeCount);
        properties = new PoolProperties();
        properties.setName(config.getMhaName());
        properties.setUrl(config.getTarget().getURL());
        properties.setUsername(config.getTarget().getUsername());
        properties.setPassword(config.getTarget().getPassword());
//        properties.setDefaultAutoCommit(false);
        String timeout = String.format("connectTimeout=%s;socketTimeout=60000", CONNECTION_TIMEOUT);
        properties.setConnectionProperties(timeout);

        properties.setValidationInterval(validationInterval);
        setCommonProperty(properties);

        properties.setMaxActive(poolSize);
        properties.setMaxIdle(poolSize);
        properties.setInitialSize(30);
        properties.setMinIdle(poolSize);

        inner = new DrcTomcatDataSource(properties);

        logger.info("[INIT DataSource] {}", config.getTarget().getURL());

        internal = new ThreadPoolExecutor(
                coreThreads, maxThreads, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxThreads), new ThreadPoolExecutor.AbortPolicy()
        );

        scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(getClass().getSimpleName() + "-" + "jdbc-test");
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    last = current;
                    current = count.get();
                    logger.info("execute count in 30s is: {}", current - last);
                } catch (Exception e) {
                    logger.error("monitor error", e);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);
    }


    @Override
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public void doStart() throws Exception {
        initTable();
        for (int i = 0; i < threadNum; i++) {
            internal.execute(new Runnable() {
                @Override
                public void run() {
                    int i = 0;
                    while (i++ < executeCount) {
                        try {
                            try (Connection connection = getConnection()) {
                                try (PreparedStatement statement = connection.prepareStatement(INSERT1k)) {
                                    if (statement.executeUpdate() == 1) {
                                        count.incrementAndGet();
                                    }
                                }
                            } catch (SQLException e) {
                                logger.error("get connection error, exception is: {}", e.getMessage());
                                break;
                            }
                        } catch (Exception ex) {
                            logger.error("execute insert error, exception is: {}", ex.getMessage());
                            break;
                        }
                    }
                }
            });
        }
    }

    public Connection getConnection() throws SQLException {
        return inner.getConnection();
    }

    @Override
    public void dispose() throws Exception {
        internal.shutdownNow();
        scheduledExecutorService.shutdownNow();
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private void initTable() throws SQLException {
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(CREATE_DB)) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement(CREATE_TABLE)) {
                statement.execute();
            }
        }
    }
}
