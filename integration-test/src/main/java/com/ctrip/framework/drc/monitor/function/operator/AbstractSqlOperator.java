package com.ctrip.framework.drc.monitor.function.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.BaseSqlOperator;
import com.ctrip.framework.drc.core.monitor.operator.DefaultReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:30.
 */
public abstract class AbstractSqlOperator extends BaseSqlOperator implements ReadSqlOperator<ReadResource> {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public AbstractSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
        super(endpoint, poolProperties);
    }

    @Override
    public ReadResource select(Execution execution) throws SQLException {
        Collection<String> statements = execution.getStatements();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        for (String sql : statements) {
            logger.info("[{}] execute [{}]", getClass().getSimpleName(), sql);
            ResultSet resultSet = statement.executeQuery(sql);
            return new DefaultReadResource(connection, statement, resultSet);
        }
        return null;
    }


    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        if ("true".equalsIgnoreCase(System.getProperty(SystemConfig.MYSQL_DB_INIT_TEST))) {
            initDbAndTables();
        }
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private void initDbAndTables() {
        dropDbAndTables();
        Statement statement = null;
        try (final Connection connection = dataSource.getConnection()) {
            statement = connection.createStatement();
            for (String sql : getInitSql()) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            logger.error("init error", e);
        } finally {
            closeStatement(statement);
        }
    }

    /**
     * 这里是最先操作的地方，可以在此判断数据库是否启动
     */
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private void dropDbAndTables() {
        Statement statement = null;
        try (final Connection connection = getConnection()) {
            statement = connection.createStatement();
            for (String sql : getDropSql()) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            logger.error("drop error", e);
        } finally {
            closeStatement(statement);
        }
    }

    private void closeStatement(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
            }
        }
    }

    private Connection getConnection() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Connection connection = dataSource.getConnection();
                return connection;
            } catch (Throwable t) {
                logger.warn("wait for db start");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                }
            }
        }
        return null;
    }

    protected Collection<String> getInitSql() {
        List<String> res = Lists.newArrayList();
        res.addAll(initSceneOne());
        res.addAll(initSceneTwo());
        res.addAll(initSceneThree());
        res.addAll(initMultiTypeNumber());
        res.addAll(initMultiTypeNumberUnsigned());
        res.addAll(initFloatPointType());
        res.addAll(initCharsetType());
        res.addAll(initTimeType());
        res.addAll(initForeignKey());
        res.addAll(initQps());
        res.addAll(initDelayMonitor());
        res.addAll(initDDL());

        return res;
    }

    private List<String> initSceneOne() {
        return Lists.newArrayList(
                "create database IF NOT EXISTS drc1;",
                "create database IF NOT EXISTS drc2;",
                "create database IF NOT EXISTS drc3;",

                "CREATE TABLE `drc1`.`insert1` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`one` varchar(30) DEFAULT \"one\"," +
                        "`two` varchar(1000) DEFAULT \"two\"," +
                        "`three` char(30)," +
                        "`four` char(255)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",

                "CREATE TABLE `drc2`.`insert1` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`one` varchar(30) DEFAULT \"one\"," +
                        "`two` varchar(1000) DEFAULT \"two\"," +
                        "`three` char(30)," +
                        "`four` char(255)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",

                "CREATE TABLE `drc3`.`insert1` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`one` varchar(30) DEFAULT \"one\"," +
                        "`two` varchar(1000) DEFAULT \"two\"," +
                        "`three` char(30)," +
                        "`four` char(255)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                "CREATE TABLE `drc1`.`insert1_uk` (\n" +
                        "                        `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                        "                        `one` varchar(30) DEFAULT \"one\",\n" +
                        "                        `two` varchar(1000) DEFAULT \"two\",\n" +
                        "                        `three` char(30),\n" +
                        "                        `four` char(255),\n" +
                        "                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'update time',\n" +
                        "                        PRIMARY KEY (`id`),\n" +
                        "                        UNIQUE KEY `uniq_one_date` (`one`, `datachange_lasttime`)\n" +
                        "                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"

        );
    }

    private List<String> initSceneTwo() {
        return Lists.newArrayList(
                "create database IF NOT EXISTS drc4;",

                "CREATE TABLE `drc4`.`insert1` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`one` varchar(30) DEFAULT \"one\"," +
                        "`two` varchar(1000) DEFAULT \"two\"," +
                        "`three` char(30)," +
                        "`four` char(255)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",

                "CREATE TABLE `drc4`.`insert2` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`one` varchar(30) DEFAULT \"one\"," +
                        "`two` varchar(1000) DEFAULT \"two\"," +
                        "`three` char(30)," +
                        "`four` char(255)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",

                "CREATE TABLE `drc4`.`insert3` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`one` varchar(30) DEFAULT \"one\"," +
                        "`two` varchar(1000) DEFAULT \"two\"," +
                        "`three` char(30)," +
                        "`four` char(255)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;",

                "CREATE TABLE `drc4`.`update1` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`one` varchar(30) DEFAULT \"one\"," +
                        "`two` varchar(1000) DEFAULT \"two\"," +
                        "`three` char(30)," +
                        "`four` char(255)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"

        );
    }

    private List<String> initSceneThree() {
        return Lists.newArrayList(
                "CREATE TABLE `drc4`.`component` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`charlt256` char(30) CHARACTER SET gbk," +
                        "`chareq256` char(128) CHARACTER SET cp932," +
                        "`chargt256` char(255) CHARACTER SET euckr," +
                        "`varcharlt256` varchar(30) CHARACTER SET utf8mb4," +
                        "`varchareq256` varchar(256) CHARACTER SET latin1," +
                        "`varchargt256` varchar(5000) CHARACTER SET utf8," +
                        "`tinyint` tinyint(5)," +
                        "`bigint` bigint(100)," +
                        "`integer` integer(50)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;"

        );
    }

    private List<String> initMultiTypeNumber() {
        return Lists.newArrayList(
                "CREATE TABLE `drc4`.`multi_type_number` (" +
                "  `bit1` bit(8) unique," +
                "  `bit2` bit(16)," +
                "  `bit3` bit(24)," +
                "  `bit4` bit(32)," +
                "  `bit5` bit(40)," +
                "  `bit6` bit(48)," +
                "  `bit7` bit(56)," +
                "  `bit8` bit(64)," +
                "  `tinyint` tinyint(5)," +
                "  `smallint` smallint(10)," +
                "  `mediumint` mediumint(15)," +
                "  `int` int(20)," +
                "  `integer` int(20)," +
                "  `bigint` bigint(100)," +
                "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'" +
                ") ENGINE=InnoDB;");
    }

    private List<String> initMultiTypeNumberUnsigned() {
        return Lists.newArrayList(
                "CREATE TABLE `drc4`.`multi_type_number_unsigned` (" +
                        "  `bit1` bit(8) unique," +
                        "  `bit2` bit(16)," +
                        "  `bit3` bit(24)," +
                        "  `bit4` bit(32)," +
                        "  `bit5` bit(40)," +
                        "  `bit6` bit(48)," +
                        "  `bit7` bit(56)," +
                        "  `bit8` bit(64)," +
                        "  `tinyint` tinyint(5) UNSIGNED," +
                        "  `smallint` smallint(10) UNSIGNED," +
                        "  `mediumint` mediumint(15) UNSIGNED," +
                        "  `int` int(20) UNSIGNED," +
                        "  `integer` int(20) UNSIGNED," +
                        "  `bigint` bigint(100) UNSIGNED," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'" +
                        ") ENGINE=InnoDB;");
    }

    private List<String> initFloatPointType() {
        return Lists.newArrayList(
                "CREATE TABLE `drc4`.`float_type` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`real` real," +
                        "`real10_4` real(10,4)," +
                        "`double` double," +
                        "`double10_4` double(10,4)," +
                        "`float` float," +
                        "`float10_4` float(10,4)," +
                        "  `decimal_m_max_d_min_positive` decimal(65,0) DEFAULT NULL," +
                        "  `decimal_m_max_d_min_nagetive` decimal(65,0) DEFAULT NULL," +
                        "  `decimal_d_max_positive` decimal(30,30) DEFAULT NULL," +
                        "  `decimal_d_max_nagetive` decimal(30,30) DEFAULT NULL," +
                        "  `decimal_m_max_d_max_positive` decimal(65,30) DEFAULT NULL," +
                        "  `decimal_m_max_d_max_nagetive` decimal(65,30) DEFAULT NULL," +
                        "  `decimal_positive_max` decimal(65,0) DEFAULT NULL," +
                        "  `decimal_positive_min` decimal(30,30) DEFAULT NULL," +
                        "  `decimal_nagetive_max` decimal(30,30) DEFAULT NULL," +
                        "  `decimal_nagetive_min` decimal(65,0) DEFAULT NULL," +
                        "`numeric` numeric," +
                        "`numeric10_4` numeric(10,4)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB;");
    }

    private List<String> initCharsetType() {
        return Lists.newArrayList(
                "CREATE TABLE `drc4`.`charset_type` (" +
                        " `id` int(11) NOT NULL AUTO_INCREMENT," +
                        "  `varchar4000` varchar(1000) CHARACTER SET utf8mb4," +
                        "  `char1000` char(250) CHARACTER SET utf8mb4," +
                        "  `varbinary1800` varbinary(1800)," +
                        "  `binary200` binary(200)," +
                        "  `tinyblob` tinyblob," +
                        "  `mediumblob` mediumblob," +
                        "  `blob` blob," +
                        "  `longblob` longblob," +
                        "  `tinytext` tinytext CHARACTER SET utf8mb4," +
                        "  `mediumtext` mediumtext CHARACTER SET utf8mb4," +
                        "  `text` text CHARACTER SET utf8mb4," +
                        "  `longtext` longtext CHARACTER SET utf8mb4," +
                        "  `longtextwithoutcharset` longtext," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "   PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;");
    }

    private List<String> initTimeType() {
        return Lists.newArrayList(
                "CREATE TABLE `drc4`.`time_type` (" +
                        "`date` date," +
                        "`time` time," +
                        "`time6` time(6)," +
                        "`datetime` datetime DEFAULT CURRENT_TIMESTAMP," +
                        "`datetime6` datetime(6)," +
                        "`timestamp` timestamp DEFAULT CURRENT_TIMESTAMP," +
                        "`timestamp6` timestamp(6) NULL," +
                        "`year` year," +
                        "`year4` year(4)," +
	                    "`appid` int(20) not null unique," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'" +
                        ") ENGINE=InnoDB;",
                
                "CREATE TABLE `drc4`.`time_type_boundary` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`date_min` date," +
                        "`date_max` date," +
                        "`time_min` time," +
                        "`time_max` time," +
                        "`time1` time(1)," +
                        "`time3` time(3)," +
                        "`time5` time(5)," +
                        "`time6_min` time(6)," +
                        "`time6_max` time(6)," +
                        "`datetime_min` datetime," +
                        "`datetime_max` datetime," +
                        "`datetime1` datetime(1)," +
                        "`datetime3` datetime(3)," +
                        "`datetime5` datetime(5)," +
                        "`datetime6_min` datetime(6)," +
                        "`datetime6_max` datetime(6)," +
                        "`timestamp_min` timestamp NULL," +
                        "`timestamp_max` timestamp NULL," +
                        "`timestamp1` timestamp(1) NULL," +
                        "`timestamp3` timestamp(3) NULL," +
                        "`timestamp5` timestamp(5) NULL," +
                        "`timestamp6_min` timestamp(6) NULL," +
                        "`timestamp6_max` timestamp(6) NULL," +
                        "`year_min` year," +
                        "`year_max` year," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB;"
                );
    }

    private List<String> initQps() {
        return Lists.newArrayList(
                "CREATE TABLE `drc4`.`benchmark` (" +
                        "`id` int(11) NOT NULL AUTO_INCREMENT," +
                        "`charlt256` char(30) CHARACTER SET gbk," +
                        "`chareq256` char(128) CHARACTER SET cp932," +
                        "`chargt256` char(255) CHARACTER SET euckr," +
                        "`varcharlt256` varchar(30) CHARACTER SET utf8mb4," +
                        "`varchareq256` varchar(256) CHARACTER SET latin1," +
                        "`varchargt256` varchar(12000) CHARACTER SET utf8," +
                        "`tinyint` tinyint(5)," +
                        "`bigint` bigint(100)," +
                        "`integer` integer(50)," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'," +
                        "PRIMARY KEY (`id`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;"

        );
    }

    private List<String> initDelayMonitor() {
        return Lists.newArrayList(
                "CREATE DATABASE IF NOT EXISTS drcmonitordb;",

                "CREATE TABLE IF NOT EXISTS `drcmonitordb`.`delaymonitor` (" +
                        "`id` bigint(20) NOT NULL AUTO_INCREMENT," +
                        "`src_ip` varchar(15) NOT NULL," +
                        "`dest_ip` varchar(15) NOT NULL," +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)," +
                        "PRIMARY KEY(`id`)" +
                        ") ENGINE=InnoDB;"
        );
    }

    private List<String> initForeignKey() {
        return Lists.newArrayList(
                "SET FOREIGN_KEY_CHECKS=0;",
                "CREATE TABLE `drc4`.`answer` (\n" +
                        "  `a_id` numeric(10,0),\n" +
                        "  `q_id` numeric(10,0) not null,\n" +
                        "  `best_answer` numeric(1) default 0 not null,\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                        " primary key(a_id),\n" +
                        " foreign key (q_id) references question(q_id)\n" +
                        ") ENGINE=InnoDB;",

                "CREATE TABLE `drc4`.`question` (\n" +
                        "  `q_id` numeric(10,0),\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                        " primary key(q_id)\n" +
                        ") ENGINE=InnoDB;"
        );
    }

    private List<String> initDDL() {
        return Lists.newArrayList(
                "create database if not exists bbzdrcbenchmarkdb character set utf8;",

                "CREATE TABLE IF NOT EXISTS `bbzdrcbenchmarkdb`.`benchmark1` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '空',\n" +
                        "  `charlt256` char(30) DEFAULT NULL COMMENT '空',\n" +
                        "  `chareq256` char(128) DEFAULT NULL COMMENT '空',\n" +
                        "  `chargt256` char(255) DEFAULT NULL COMMENT '空',\n" +
                        "  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '空',\n" +
                        "  `varchareq256` varchar(256) DEFAULT NULL COMMENT '空',\n" +
                        "  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '空',\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                        "  `drc_id_int` int(11) NOT NULL DEFAULT '1' COMMENT '空',\n" +
                        "  `addcol1` varchar(64) DEFAULT 'default_addcol1' COMMENT 'test',\n" +
                        "  `addcol2` varchar(64) DEFAULT 'default_addcol2' COMMENT 'test',\n" +
                        "  `drc_char_test_2` char(30) DEFAULT 'char' COMMENT '空',\n" +
                        "  `drc_tinyint_test_2` tinyint(4) DEFAULT '12' COMMENT '空',\n" +
                        "  `drc_bigint_test` bigint(20) DEFAULT '120' COMMENT '空',\n" +
                        "  `drc_integer_test` int(11) DEFAULT '11' COMMENT '空',\n" +
                        "  `drc_mediumint_test` mediumint(9) DEFAULT '12345' COMMENT '空',\n" +
                        "  `drc_time6_test` time DEFAULT '02:02:02' COMMENT '空',\n" +
                        "  `drc_datetime3_test` datetime(3) DEFAULT '2019-01-01 01:01:01.000' COMMENT '空',\n" +
                        "  `drc_year_test` year(4) DEFAULT '2020' COMMENT '空',\n" +
                        "  `hourly_rate_3` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT '空',\n" +
                        "  `drc_numeric10_4_test` decimal(10,4) DEFAULT '100.0000' COMMENT '空',\n" +
                        "  `drc_float_test` float DEFAULT '12' COMMENT '空',\n" +
                        "  `drc_double_test` double DEFAULT '123' COMMENT '空',\n" +
                        "  `drc_bit4_test` bit(4) DEFAULT b'11' COMMENT 'TEST',\n" +
                        "  `drc_double10_4_test` double(10,4) DEFAULT '123.1245' COMMENT '空',\n" +
                        "  `drc_real_test` double DEFAULT '234' COMMENT '空',\n" +
                        "  `drc_real10_4_test` double(10,4) DEFAULT '23.4000' COMMENT '空',\n" +
                        "  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0' COMMENT '空',\n" +
                        "  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '空',\n" +
                        "  `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';"
        );
    }

    protected Collection<String> getDropSql() {
        return Lists.newArrayList(
                "drop database if exists drc1;",
                "drop database if exists drc2;",
                "drop database if exists drc3;",
                "drop database if exists drc4;",
                "drop database if exists drcmetadb;"
        );
    }
}
