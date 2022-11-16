package com.ctrip.framework.drc.replicator.impl;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.TableCreateTask;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.DbInitTask;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.wix.mysql.EmbeddedMysql;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 1、create 4000 tables by 50 thread
 * 2、create 4000 partitioned tables by 50 thread
 *
 * @Author limingdong
 * @create 2022/11/8
 */
public class PartitionTableCreateTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final int PORT = 8900;

    private static final int TABLE_COUNT = 50;

    private static final String CREATE_DB1 = "CREATE DATABASE drc1";
    private static final String CREATE_DB2 = "CREATE DATABASE drc2";
    private static final String CREATE_DB3 = "CREATE DATABASE drc3";

    private EmbeddedMysql embeddedMysql;

    private DataSource dataSource;

    private Endpoint endpoint;

    private List<Long> times = new ArrayList<>();

    private static final String CREATE_TABLE = "CREATE TABLE `%s`.`transfer_log_%d` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '',\n" +
            "  `uxifd` varchar(20) NOT NULL DEFAULT '' COMMENT '',\n" +
            "  `status` int(11) DEFAULT NULL COMMENT '',\n" +
            "  `message` varchar(50) DEFAULT NULL COMMENT '',\n" +
            "  `batchNo` varchar(5) DEFAULT NULL COMMENT '',\n" +
            "  `statusDescrip` varchar(50) DEFAULT NULL COMMENT '',\n" +
            "  `datachange_lasttime` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '',\n" +
            "  PRIMARY KEY (`id`,`datachange_lasttime`),\n" +
            "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`),\n" +
            "  KEY `ix_uid` (`uid`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";


    private static final String CREATE_PARTITION_TABLE = " CREATE TABLE `%s`.`transfer_log_%d` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '',\n" +
            "  `uxifd` varchar(20) NOT NULL DEFAULT '' COMMENT '',\n" +
            "  `status` int(11) DEFAULT NULL COMMENT '',\n" +
            "  `message` varchar(50) DEFAULT NULL COMMENT '',\n" +
            "  `batchNo` varchar(5) DEFAULT NULL COMMENT '',\n" +
            "  `statusDescrip` varchar(50) DEFAULT NULL COMMENT '',\n" +
            "  `datachange_lasttime` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '',\n" +
            "  PRIMARY KEY (`id`,`datachange_lasttime`),\n" +
            "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`),\n" +
            "  KEY `ix_uid` (`uid`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT=''\n" +
            "/*!50100 PARTITION BY RANGE (to_days(datachange_lasttime))\n" +
            "(PARTITION p20220321 VALUES LESS THAN (738607) ENGINE = InnoDB,\n" +
            " PARTITION p20220328 VALUES LESS THAN (738614) ENGINE = InnoDB,\n" +
            " PARTITION p20220404 VALUES LESS THAN (738621) ENGINE = InnoDB,\n" +
            " PARTITION p20220411 VALUES LESS THAN (738628) ENGINE = InnoDB,\n" +
            " PARTITION p20220418 VALUES LESS THAN (738635) ENGINE = InnoDB,\n" +
            " PARTITION p20220425 VALUES LESS THAN (738642) ENGINE = InnoDB,\n" +
            " PARTITION p20220502 VALUES LESS THAN (738649) ENGINE = InnoDB,\n" +
            " PARTITION p20220509 VALUES LESS THAN (738656) ENGINE = InnoDB,\n" +
            " PARTITION p20220516 VALUES LESS THAN (738663) ENGINE = InnoDB,\n" +
            " PARTITION p20220523 VALUES LESS THAN (738670) ENGINE = InnoDB,\n" +
            " PARTITION p20220530 VALUES LESS THAN (738677) ENGINE = InnoDB,\n" +
            " PARTITION p20220606 VALUES LESS THAN (738684) ENGINE = InnoDB,\n" +
            " PARTITION p20220613 VALUES LESS THAN (738691) ENGINE = InnoDB,\n" +
            " PARTITION p20220620 VALUES LESS THAN (738698) ENGINE = InnoDB,\n" +
            " PARTITION p20220627 VALUES LESS THAN (738705) ENGINE = InnoDB,\n" +
            " PARTITION p20220704 VALUES LESS THAN (738712) ENGINE = InnoDB,\n" +
            " PARTITION p20220711 VALUES LESS THAN (738719) ENGINE = InnoDB,\n" +
            " PARTITION p20220718 VALUES LESS THAN (738726) ENGINE = InnoDB,\n" +
            " PARTITION p20220725 VALUES LESS THAN (738733) ENGINE = InnoDB,\n" +
            " PARTITION p20220801 VALUES LESS THAN (738740) ENGINE = InnoDB,\n" +
            " PARTITION p20220808 VALUES LESS THAN (738747) ENGINE = InnoDB,\n" +
            " PARTITION p20220815 VALUES LESS THAN (738754) ENGINE = InnoDB,\n" +
            " PARTITION p20220822 VALUES LESS THAN (738761) ENGINE = InnoDB,\n" +
            " PARTITION p20220829 VALUES LESS THAN (738768) ENGINE = InnoDB,\n" +
            " PARTITION p20220905 VALUES LESS THAN (738775) ENGINE = InnoDB,\n" +
            " PARTITION p20220912 VALUES LESS THAN (738782) ENGINE = InnoDB,\n" +
            " PARTITION p20220919 VALUES LESS THAN (738789) ENGINE = InnoDB,\n" +
            " PARTITION p20220926 VALUES LESS THAN (738796) ENGINE = InnoDB,\n" +
            " PARTITION p20221003 VALUES LESS THAN (738803) ENGINE = InnoDB,\n" +
            " PARTITION p20221010 VALUES LESS THAN (738810) ENGINE = InnoDB,\n" +
            " PARTITION p20221017 VALUES LESS THAN (738817) ENGINE = InnoDB,\n" +
            " PARTITION p20221031 VALUES LESS THAN (738831) ENGINE = InnoDB,\n" +
            " PARTITION p20221107 VALUES LESS THAN (738838) ENGINE = InnoDB,\n" +
            " PARTITION p20221114 VALUES LESS THAN (738845) ENGINE = InnoDB,\n" +
            " PARTITION p20221121 VALUES LESS THAN (738852) ENGINE = InnoDB,\n" +
            " PARTITION p20221128 VALUES LESS THAN (738859) ENGINE = InnoDB,\n" +
            " PARTITION p20221205 VALUES LESS THAN (738866) ENGINE = InnoDB,\n" +
            " PARTITION p20221212 VALUES LESS THAN (738873) ENGINE = InnoDB,\n" +
            " PARTITION p20221219 VALUES LESS THAN (738880) ENGINE = InnoDB,\n" +
            " PARTITION p20221226 VALUES LESS THAN (738887) ENGINE = InnoDB,\n" +
            " PARTITION p20230102 VALUES LESS THAN (738894) ENGINE = InnoDB,\n" +
            " PARTITION p20230109 VALUES LESS THAN (738901) ENGINE = InnoDB,\n" +
            " PARTITION p20230116 VALUES LESS THAN (738908) ENGINE = InnoDB,\n" +
            " PARTITION p20230123 VALUES LESS THAN (738915) ENGINE = InnoDB,\n" +
            " PARTITION p20230130 VALUES LESS THAN (738922) ENGINE = InnoDB,\n" +
            " PARTITION pMax VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */";

    @Before
    public void setUp() throws SQLException {
        embeddedMysql = new RetryTask<>(new DbInitTask(PORT, "ParititionTableCreateCase")).call();
        endpoint = new DefaultEndPoint("127.0.0.1", PORT, "root", "");
        dataSource = DataSourceManager.getInstance().getDataSource(endpoint);
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(CREATE_DB1);
                statement.execute(CREATE_DB2);
                statement.execute(CREATE_DB3);
            }
        }

    }

    @After
    public void tearDown() {
        embeddedMysql.stop();
    }

    @Test
    public void test() throws Exception {
        execute(testCase1());
        execute(testCase2());
        log();
    }

    private void log() {
        for (Long time : times) {
            logger.info("use {}ms", time);
        }
    }

    public void execute(TableCreateTask tableCreateTask) throws Exception {
        long start = System.currentTimeMillis();
        tableCreateTask.call();
        long end = System.currentTimeMillis();
        logger.info("{} use {}ms", tableCreateTask.name(), end - start);
        times.add(end - start);
    }

    private TableCreateTask testCase1() {
        List<String> sqls = new ArrayList<>();
        for (int i = 0; i < TABLE_COUNT; ++i) {
            sqls.add(String.format(CREATE_TABLE, "drc1", i));
        }
        return new TableCreateTask(sqls, endpoint, dataSource);
    }

    private TableCreateTask testCase2() {
        List<String> sqls = new ArrayList<>();
        for (int i = 0; i < TABLE_COUNT; ++i) {
            sqls.add(String.format(CREATE_PARTITION_TABLE, "drc2", i));
        }
        return new TableCreateTask(sqls, endpoint, dataSource);
    }


}
