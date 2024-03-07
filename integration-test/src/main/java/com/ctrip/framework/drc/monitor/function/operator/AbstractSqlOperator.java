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
        res.addAll(initTypeModify());
        res.addAll(initRowsFilter());
        res.addAll(initGrandTransaction());
        res.addAll(initForeignKey());
        res.addAll(initQps());
        res.addAll(initDelayMonitor());
        res.addAll(initTransactionTable());
        res.addAll(initDDL());
        res.addAll(initConfigDb());
        res.addAll(initDrc1dbJsonCase());
        System.out.println("json table created");
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

    public List<String> initDrc1dbJsonCase() {
        return Lists.newArrayList(
                "CREATE TABLE if not exists `drc1`.`json` (\n" +
                        "`id` int(11) AUTO_INCREMENT PRIMARY KEY,\n" +
                        "`json_data` JSON,\n" +
                        "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间'" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
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
                "CREATE TABLE `drc4`.`multi_type_number` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                        "  `bit1` bit(8) unique,\n" +
                        "  `bit2` bit(16),\n" +
                        "  `bit3` bit(24),\n" +
                        "  `bit4` bit(32),\n" +
                        "  `bit5` bit(40),\n" +
                        "  `bit6` bit(48),\n" +
                        "  `bit7` bit(56),\n" +
                        "  `bit8` bit(64),\n" +
                        "  `tinyint` tinyint(5),\n" +
                        "  `smallint` smallint(10),\n" +
                        "  `mediumint` mediumint(15),\n" +
                        "  `int` int(20),\n" +
                        "  `integer` int(20),\n" +
                        "  `bigint` bigint(100),\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB;");
    }

    private List<String> initMultiTypeNumberUnsigned() {
        return Lists.newArrayList(
                "CREATE TABLE `drc4`.`multi_type_number_unsigned` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                        "  `bit1` bit(8) unique,\n" +
                        "  `bit2` bit(16),\n" +
                        "  `bit3` bit(24),\n" +
                        "  `bit4` bit(32),\n" +
                        "  `bit5` bit(40),\n" +
                        "  `bit6` bit(48),\n" +
                        "  `bit7` bit(56),\n" +
                        "  `bit8` bit(64),\n" +
                        "  `tinyint` tinyint(5) UNSIGNED,\n" +
                        "  `smallint` smallint(10) UNSIGNED,\n" +
                        "  `mediumint` mediumint(15) UNSIGNED,\n" +
                        "  `int` int(20) UNSIGNED,\n" +
                        "  `integer` int(20) UNSIGNED,\n" +
                        "  `bigint` bigint(100) UNSIGNED,\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                        "  PRIMARY KEY (`id`)\n" +
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
                "CREATE TABLE `drc4`.`charset_type` (\n" +
                        "  `varchar4000` varchar(1000) CHARACTER SET utf8mb4,\n" +
                        "  `char1000` char(250) CHARACTER SET utf8mb4,\n" +
                        "  `varbinary1800` varbinary(1800),\n" +
                        "  `binary200` binary(200),\n" +
                        "  `tinyblob` tinyblob,\n" +
                        "  `mediumblob` mediumblob,\n" +
                        "  `blob` blob,\n" +
                        "  `longblob` longblob,\n" +
                        "  `tinytext` tinytext CHARACTER SET utf8mb4,\n" +
                        "  `mediumtext` mediumtext CHARACTER SET utf8mb4,\n" +
                        "  `text` text CHARACTER SET utf8mb4,\n" +
                        "  `longtext` longtext CHARACTER SET utf8mb4,\n" +
                        "  `longtextwithoutcharset` longtext CHARACTER SET latin1,\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB;");
    }

    private List<String> initTimeType() {
        return Lists.newArrayList(
                "CREATE TABLE `drc4`.`time_type` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                        "  `date` date,\n" +
                        "\t`time` time,\n" +
                        "\t`time6` time(6),\n" +
                        "\t`datetime` datetime DEFAULT CURRENT_TIMESTAMP,\n" +
                        "\t`datetime6` datetime(6),\n" +
                        "\t`timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n" +
                        "\t`timestamp6` timestamp(6) NULL,\n" +
                        "\t`year` year,\n" +
                        "\t`year4` year(4),\n" +
                        "\t`appid` int(20) not null unique,\n" +
                        "\t`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                        "\tPRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB;",

                "CREATE TABLE `drc4`.`time_type_boundary` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                        "  `date_min` date,\n" +
                        "  `date_max` date,\n" +
                        "\t`time_min` time,\n" +
                        "\t`time_max` time,\n" +
                        "\t`time1` time(1),\n" +
                        "\t`time3` time(3),\n" +
                        "\t`time5` time(5),\n" +
                        "\t`time6_min` time(6),\n" +
                        "\t`time6_max` time(6),\n" +
                        "\t`datetime_min` datetime,\n" +
                        "\t`datetime_max` datetime,\n" +
                        "\t`datetime1` datetime(1),\n" +
                        "\t`datetime3` datetime(3),\n" +
                        "\t`datetime5` datetime(5),\n" +
                        "\t`datetime6_min` datetime(6),\n" +
                        "\t`datetime6_max` datetime(6),\n" +
                        "\t`timestamp_min` timestamp NULL,\n" +
                        "\t`timestamp_max` timestamp NULL,\n" +
                        "\t`timestamp1` timestamp(1) NULL,\n" +
                        "\t`timestamp3` timestamp(3) NULL,\n" +
                        "\t`timestamp5` timestamp(5) NULL,\n" +
                        "\t`timestamp6_min` timestamp(6) NULL,\n" +
                        "\t`timestamp6_max` timestamp(6) NULL,\n" +
                        "\t`year_min` year,\n" +
                        "\t`year_max` year,\n" +
                        "\t`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                        "\tPRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB;"
                );
    }

    private List<String> initTypeModify() {
        return Lists.newArrayList(
                "CREATE DATABASE IF NOT EXISTS drc4;",

                "CREATE TABLE `drc4`.`table_map` (\n" +
                        "  `id` int(11) not null AUTO_INCREMENT,\n" +
                        "  `size` enum('x-small','small','medium','large','x-large','aaaaaaaaaaaa') DEFAULT 'x-small',\n" +
                        "  `size_big` enum('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65','66','67','68','69','70','71','72','73','74','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91','92','93','94','95','96','97','98','99','100','101','102','103','104','105','106','107','108','109','110','111','112','113','114','115','116','117','118','119','120','121','122','123','124','125','126','127','128','129','130','131','132','133','134','135','136','137','138','139','140','141','142','143','144','145','146','147','148','149','150','151','152','153','154','155','156','157','158','159','160','161','162','163','164','165','166','167','168','169','170','171','172','173','174','175','176','177','178','179','180','181','182','183','184','185','186','187','188','189','190','191','192','193','194','195','196','197','198','199','200','201','202','203','204','205','206','207','208','209','210','211','212','213','214','215','216','217','218','219','220','221','222','223','224','225','226','227','228','229','230','231','232','233','234','235','236','237','238','239','240','241','242','243','244','245','246','247','248','249','250','251','252','253','254','255','256','257','258','259','260') DEFAULT '1',\n" +
                        "  `decimal_max` decimal(65,30),\n" +
                        "  `varcharlt256_utf8mb4` varchar(60) CHARACTER SET utf8mb4,\n" +
                        "  `varchargt256_utf8mb4` varchar(65) CHARACTER SET utf8mb4,\n" +
                        "  `char240_utf8mb4` char(60) CHARACTER SET utf8mb4,\n" +
                        "  `varcharlt256_utf8` varchar(60) CHARACTER SET utf8,\n" +
                        "  `varchargt256_utf8` varchar(65) CHARACTER SET utf8,\n" +
                        "  `char240_utf8` char(60) CHARACTER SET utf8,\n" +
                        "  `varcharlt256_cp932` varchar(120) CHARACTER SET cp932,\n" +
                        "  `varchargt256_cp932` varchar(250) CHARACTER SET cp932,\n" +
                        "  `char240_cp932` char(60) CHARACTER SET cp932,\n" +
                        "  `varcharlt256_euckr` varchar(120) CHARACTER SET euckr,\n" +
                        "  `varchargt256_euckr` varchar(250) CHARACTER SET euckr,\n" +
                        "  `char240_euckr` char(60) CHARACTER SET euckr,\n" +
                        "  `varbinarylt256` varbinary(240),\n" +
                        "  `varbinarygt256` varbinary(260),\n" +
                        "  `binary200` binary(200),\n" +
                        "  `tinyintunsigned` tinyint(5) UNSIGNED,\n" +
                        "  `smallintunsigned` smallint(10) UNSIGNED,\n" +
                        "  `mediumintunsigned` mediumint(15) UNSIGNED,\n" +
                        "  `intunsigned` int(20) UNSIGNED,\n" +
                        "  `bigintunsigned` bigint(100) UNSIGNED,\n" +
                        "  `tinyint` tinyint(5),\n" +
                        "  `smallint` smallint(10),\n" +
                        "  `mediumint` mediumint(15),\n" +
                        "  `int` int(20),\n" +
                        "  `bigint` bigint(100),\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                        "  PRIMARY KEY (`id`)\n" +
                        ") ENGINE=InnoDB;"
        );
    }

    private List<String> initRowsFilter() {
        return Lists.newArrayList(
                "CREATE DATABASE IF NOT EXISTS drc4;",

                "CREATE TABLE `drc4`.`row_filter` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '',\n" +
                        "  `uid` char(30) DEFAULT NULL COMMENT '',\n" +
                        "  `charlt256` char(30) DEFAULT NULL COMMENT '',\n" +
                        "  `chareq256` char(128) DEFAULT NULL COMMENT '',\n" +
                        "  `chargt256` char(255) DEFAULT NULL COMMENT '',\n" +
                        "  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '',\n" +
                        "  `varchareq256` varchar(256) DEFAULT NULL COMMENT '',\n" +
                        "  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '',\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
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
                        "  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002' COMMENT '',\n" +
                        "  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '',\n" +
                        "  `addcol` varchar(50) DEFAULT 'addColName' COMMENT 'Name',\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';",
                "CREATE TABLE `drc4`.`row_filter_udl` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '',\n" +
                        "  `uid` char(30) DEFAULT NULL COMMENT '',\n" +
                        "  `udl` char(30) DEFAULT NULL COMMENT '',\n" +
                        "  `charlt256` char(30) DEFAULT NULL COMMENT '',\n" +
                        "  `chareq256` char(128) DEFAULT NULL COMMENT '',\n" +
                        "  `chargt256` char(255) DEFAULT NULL COMMENT '',\n" +
                        "  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '',\n" +
                        "  `varchareq256` varchar(256) DEFAULT NULL COMMENT '',\n" +
                        "  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '',\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
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
                        "  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002' COMMENT '',\n" +
                        "  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '',\n" +
                        "  `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';"
                );
    }

    private List<String> initGrandTransaction() {
        return Lists.newArrayList(
                "CREATE DATABASE IF NOT EXISTS drc4;",

                "CREATE TABLE `drc4`.`grand_transaction` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '',\n" +
                        "  `charlt256` char(30) DEFAULT NULL COMMENT '',\n" +
                        "  `chareq256` char(128) DEFAULT NULL COMMENT '',\n" +
                        "  `chargt256` char(255) DEFAULT NULL COMMENT '',\n" +
                        "  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '',\n" +
                        "  `varchareq256` varchar(256) DEFAULT NULL COMMENT '',\n" +
                        "  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '',\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
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
                        "  `drc_bit4_test` bit(1) DEFAULT b'1' COMMENT 'TEST',\n" +
                        "  `drc_double10_4_test` double(10,4) DEFAULT '123.1245' COMMENT '',\n" +
                        "  `drc_real_test` double DEFAULT '234' COMMENT '',\n" +
                        "  `drc_real10_4_test` double(10,4) DEFAULT '23.4000' COMMENT '',\n" +
                        "  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0' COMMENT '',\n" +
                        "  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '',\n" +
                        "  `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='function_grand_transaction_test';"
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

    private List<String> initTransactionTable() {
        return Lists.newArrayList(
                "CREATE DATABASE IF NOT EXISTS drcmonitordb;",

                "CREATE TABLE IF NOT EXISTS `drcmonitordb`.`gtid_executed` (\n" +
                        "  `id` int(11) NOT NULL,\n" +
                        "  `server_uuid` char(36) NOT NULL,\n" +
                        "  `gno` bigint(20) NOT NULL,\n" +
                        "  `gtidset` longtext,\n" +
                        "  PRIMARY KEY ix_gtid(`id`,`server_uuid`)\n" +
                        ");"
        );
    }

    private List<String> initForeignKey() {
        return Lists.newArrayList(
                "SET FOREIGN_KEY_CHECKS=0;",
                "CREATE TABLE `drc4`.`answer` (\n" +
                        "  `a_id` numeric(10,0),\n" +
                        "  `q_id` numeric(10,0) not null,\n" +
                        "  `best_answer` numeric(1) default 0 not null,\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'time',\n" +
                        " primary key(a_id),\n" +
                        " foreign key (q_id) references question(q_id)\n" +
                        ") ENGINE=InnoDB;",

                "CREATE TABLE `drc4`.`question` (\n" +
                        "  `q_id` numeric(10,0),\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'time',\n" +
                        " primary key(q_id)\n" +
                        ") ENGINE=InnoDB;"
        );
    }

    private List<String> initDDL() {
        return Lists.newArrayList(
                "create database if not exists bbzdrcbenchmarkdb character set utf8;",

                "CREATE TABLE IF NOT EXISTS `bbzdrcbenchmarkdb`.`benchmark1` (\n" +
                        "  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '',\n" +
                        "  `charlt256` char(30) DEFAULT NULL COMMENT '',\n" +
                        "  `chareq256` char(128) DEFAULT NULL COMMENT '',\n" +
                        "  `chargt256` char(255) DEFAULT NULL COMMENT '',\n" +
                        "  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '',\n" +
                        "  `varchareq256` varchar(256) DEFAULT NULL COMMENT '',\n" +
                        "  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '',\n" +
                        "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
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
                        "  `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',\n" +
                        "  PRIMARY KEY (`id`),\n" +
                        "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';"
        );
    }

    private List<String> initConfigDb() {
        return Lists.newArrayList(
                "CREATE DATABASE IF NOT EXISTS configdb;",
                "CREATE TABLE `configdb`.`heartbeat` (\n" +
                        "  `hostname` varchar(50) NOT NULL DEFAULT '',\n" +
                        "  `time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n" +
                        "  PRIMARY KEY (`time`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        );
    }

    protected Collection<String> getDropSql() {
        return Lists.newArrayList(
                "drop database if exists drc1;",
                "drop database if exists drc2;",
                "drop database if exists drc3;",
                "drop database if exists drc4;"
        );
    }
}
