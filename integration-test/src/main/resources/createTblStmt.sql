-- 场景1：事务多个事件跨库
create database if not exists drc1;

create database if not exists drc2;

create database if not exists drc3;

create database if not exists generic_ddl;

create database if not exists ghost1_unitest;

CREATE TABLE `drc1`.`insert1` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `one` varchar(30) DEFAULT "one",
                        `two` varchar(1000) DEFAULT "two",
                        `three` char(30),
                        `four` char(255),
                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
                        PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `drc1`.`insert1_uk` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `one` varchar(30) DEFAULT "one",
                        `two` varchar(1000) DEFAULT "two",
                        `three` char(30),
                        `four` char(255),
                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `uniq_one_date` (`one`, `datachange_lasttime`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `drc2`.`insert1` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `one` varchar(30) DEFAULT "one",
                        `two` varchar(1000) DEFAULT "two",
                        `three` char(30),
                        `four` char(255),
                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
                        PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `drc3`.`insert1` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `one` varchar(30) DEFAULT "one",
                        `two` varchar(1000) DEFAULT "two",
                        `three` char(30),
                        `four` char(255),
                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
                        PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 场景2：事务多个事件跨表
create database drc4;

CREATE TABLE `drc4`.`insert1` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `one` varchar(30) DEFAULT "one",
                        `two` varchar(1000) DEFAULT "two",
                        `three` char(30),
                        `four` char(255),
                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
                        PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `drc4`.`insert2` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `one` varchar(30) DEFAULT "one",
                        `two` varchar(1000) DEFAULT "two",
                        `three` char(30),
                        `four` char(255),
                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
                        PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `drc4`.`insert3` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `one` varchar(30) DEFAULT "one",
                        `two` varchar(1000) DEFAULT "two",
                        `three` char(30),
                        `four` char(255),
                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
                        PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `drc4`.`update1` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `one` varchar(30) DEFAULT "one",
                        `two` varchar(1000) DEFAULT "two",
                        `three` char(30),
                        `four` char(255),
                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
                        PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- 场景3：单个事件涉及多个行操作(包含后续多个场景)
-- gbk is china, maxlen = 2; cp932 japan, maxlen = 2; --euckr korean, maxlen = 2
-- utf8mb4 can any language, maxlen=4; latin1 just char, maxlen=1; utf8 can any language, maxlen=3
CREATE TABLE `drc4`.`component` (
  `id` int(11) not null AUTO_INCREMENT,
  `charlt256` char(30) CHARACTER SET gbk,
  `chareq256` char(128) CHARACTER SET cp932,
  `chargt256` char(255) CHARACTER SET euckr,
  `varcharlt256` varchar(30) CHARACTER SET utf8mb4,
  `varchareq256` varchar(256) CHARACTER SET latin1,
  `varchargt256` varchar(2000) CHARACTER SET utf8,
  `tinyint` tinyint(5),
  `bigint` bigint(100),
  `integer` integer(50),
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

CREATE TABLE `drc4`.`binlog_minimal_row_image` (
  `id` int(11) not null AUTO_INCREMENT,
  `charlt256` char(30) CHARACTER SET gbk,
  `chareq256` char(128) CHARACTER SET cp932,
  `chargt256` char(255) CHARACTER SET euckr,
  `varcharlt256` varchar(30) CHARACTER SET utf8mb4,
  `varchareq256` varchar(256) CHARACTER SET latin1,
  `varchargt256` varchar(2000) CHARACTER SET utf8,
  `tinyint` tinyint(5),
  `bigint` bigint(100),
  `integer` integer(50),
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

CREATE TABLE `drc4`.`binlog_noblob_row_image` (
  `id` int(11) not null AUTO_INCREMENT,
  `charlt256` char(30) CHARACTER SET gbk,
  `chareq256` char(128) CHARACTER SET cp932,
  `chargt256` char(255) CHARACTER SET euckr,
  `varcharlt256` varchar(30) CHARACTER SET utf8mb4,
  `varchareq256` varchar(256) CHARACTER SET latin1,
  `varchargt256` varchar(2000) CHARACTER SET utf8,
  `tinyint` tinyint(5),
  `bigint` bigint(100),
  `integer` integer(50),
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- back: SELECT table_schema,table_name,ordinal_position,column_name,column_type,data_type,is_nullable,character_set_name,collation_name  FROM information_schema.columns WHERE table_name = 'charset_japan2';
-- 场景5：多种数字数据类型signed
CREATE TABLE `drc4`.`multi_type_number` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bit1` bit(8) unique,
  `bit2` bit(16),
  `bit3` bit(24),
  `bit4` bit(32),
  `bit5` bit(40),
  `bit6` bit(48),
  `bit7` bit(56),
  `bit8` bit(64),
  `tinyint` tinyint(5),
  `smallint` smallint(10),
  `mediumint` mediumint(15),
  `int` int(20),
  `integer` int(20),
  `bigint` bigint(100),
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- 场景6：多种数字数据类型unsigned
CREATE TABLE `drc4`.`multi_type_number_unsigned` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bit1` bit(8) unique,
  `bit2` bit(16),
  `bit3` bit(24),
  `bit4` bit(32),
  `bit5` bit(40),
  `bit6` bit(48),
  `bit7` bit(56),
  `bit8` bit(64),
  `tinyint` tinyint(5) UNSIGNED,
  `smallint` smallint(10) UNSIGNED,
  `mediumint` mediumint(15) UNSIGNED,
  `int` int(20) UNSIGNED,
  `integer` int(20) UNSIGNED,
  `bigint` bigint(100) UNSIGNED,
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- 场景7：浮点类型
CREATE TABLE `drc4`.`float_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `real` real,
  `real10_4` real(10,4),
  `double` double,
  `double10_4` double(10,4),
  `float` float,
  `float10_4` float(10,4),
  `decimal_m_max_d_min_positive` decimal(65,0) DEFAULT NULL,
  `decimal_m_max_d_min_nagetive` decimal(65,0) DEFAULT NULL,
  `decimal_d_max_positive` decimal(30,30) DEFAULT NULL,
  `decimal_d_max_nagetive` decimal(30,30) DEFAULT NULL,
  `decimal_m_max_d_max_positive` decimal(65,30) DEFAULT NULL,
  `decimal_m_max_d_max_nagetive` decimal(65,30) DEFAULT NULL,
  `decimal_positive_max` decimal(65,0) DEFAULT NULL,
  `decimal_positive_min` decimal(30,30) DEFAULT NULL,
  `decimal_nagetive_max` decimal(30,30) DEFAULT NULL,
  `decimal_nagetive_min` decimal(65,0) DEFAULT NULL,
	`numeric` numeric,
	`numeric10_4` numeric(10,4),
	`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
	PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- 场景8：字符类型
CREATE TABLE `drc4`.`charset_type` (
  `varchar4000` varchar(1000) CHARACTER SET utf8mb4,
  `char1000` char(250) CHARACTER SET utf8mb4,
  `varbinary1800` varbinary(1800),
  `binary200` binary(200),
  `tinyblob` tinyblob,
  `mediumblob` mediumblob,
  `blob` blob,
  `longblob` longblob,
  `tinytext` tinytext CHARACTER SET utf8mb4,
  `mediumtext` mediumtext CHARACTER SET utf8mb4,
  `text` text CHARACTER SET utf8mb4,
  `longtext` longtext CHARACTER SET utf8mb4,
  `longtextwithoutcharset` longtext CHARACTER SET latin1,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- 场景9：时间类型
CREATE TABLE `drc4`.`time_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date` date,
	`time` time,
	`time6` time(6),
	`datetime` datetime DEFAULT CURRENT_TIMESTAMP,
	`datetime6` datetime(6),
	`timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,
	`timestamp6` timestamp(6) NULL,
	`year` year,
	`year4` year(4),
	`appid` int(20) not null unique,
	`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
	PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- 边界值测试
CREATE TABLE `drc4`.`time_type_boundary` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date_min` date,
  `date_max` date,
	`time_min` time,
	`time_max` time,
	`time1` time(1),
	`time3` time(3),
	`time5` time(5),
	`time6_min` time(6),
	`time6_max` time(6),
	`datetime_min` datetime,
	`datetime_max` datetime,
	`datetime1` datetime(1),
	`datetime3` datetime(3),
	`datetime5` datetime(5),
	`datetime6_min` datetime(6),
	`datetime6_max` datetime(6),
	`timestamp_min` timestamp NULL,
	`timestamp_max` timestamp NULL,
	`timestamp1` timestamp(1) NULL,
	`timestamp3` timestamp(3) NULL,
	`timestamp5` timestamp(5) NULL,
	`timestamp6_min` timestamp(6) NULL,
	`timestamp6_max` timestamp(6) NULL,
	`year_min` year,
	`year_max` year,
	`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
	PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- 80 00 00 00 00 00  00 00 00 00 00 00 00 00 00 01
-- 05 f5 e1 00 00 00  00 00 00 00 00 00 00 00

CREATE TABLE `drc4`.`benchmark` (
  `id` int(11) not null AUTO_INCREMENT,
  `charlt256` char(30) CHARACTER SET gbk,
  `chareq256` char(128) CHARACTER SET cp932,
  `chargt256` char(255) CHARACTER SET euckr,
  `varcharlt256` varchar(30) CHARACTER SET utf8mb4,
  `varchareq256` varchar(256) CHARACTER SET latin1,
  `varchargt256` varchar(12000) CHARACTER SET utf8,
  `tinyint` tinyint(5),
  `bigint` bigint(100),
  `integer` integer(50),
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

CREATE TABLE `drc4`.`table_map` (
  `id` int(11) not null AUTO_INCREMENT,
  `size` enum('x-small','small','medium','large','x-large','aaaaaaaaaaaa') DEFAULT 'x-small',
  `size_big` enum('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65','66','67','68','69','70','71','72','73','74','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91','92','93','94','95','96','97','98','99','100','101','102','103','104','105','106','107','108','109','110','111','112','113','114','115','116','117','118','119','120','121','122','123','124','125','126','127','128','129','130','131','132','133','134','135','136','137','138','139','140','141','142','143','144','145','146','147','148','149','150','151','152','153','154','155','156','157','158','159','160','161','162','163','164','165','166','167','168','169','170','171','172','173','174','175','176','177','178','179','180','181','182','183','184','185','186','187','188','189','190','191','192','193','194','195','196','197','198','199','200','201','202','203','204','205','206','207','208','209','210','211','212','213','214','215','216','217','218','219','220','221','222','223','224','225','226','227','228','229','230','231','232','233','234','235','236','237','238','239','240','241','242','243','244','245','246','247','248','249','250','251','252','253','254','255','256','257','258','259','260') DEFAULT '1',
  `decimal_max` decimal(65,30),
  `varcharlt256_utf8mb4` varchar(60) CHARACTER SET utf8mb4,
  `varchargt256_utf8mb4` varchar(65) CHARACTER SET utf8mb4,
  `char240_utf8mb4` char(60) CHARACTER SET utf8mb4,
  `varcharlt256_utf8` varchar(60) CHARACTER SET utf8,
  `varchargt256_utf8` varchar(65) CHARACTER SET utf8,
  `char240_utf8` char(60) CHARACTER SET utf8,
  `varcharlt256_cp932` varchar(120) CHARACTER SET cp932,
  `varchargt256_cp932` varchar(250) CHARACTER SET cp932,
  `char240_cp932` char(60) CHARACTER SET cp932,
  `varcharlt256_euckr` varchar(120) CHARACTER SET euckr,
  `varchargt256_euckr` varchar(250) CHARACTER SET euckr,
  `char240_euckr` char(60) CHARACTER SET euckr,
  `varbinarylt256` varbinary(240),
  `varbinarygt256` varbinary(260),
  `binary200` binary(200),
  `tinyintunsigned` tinyint(5) UNSIGNED,
  `smallintunsigned` smallint(10) UNSIGNED,
  `mediumintunsigned` mediumint(15) UNSIGNED,
  `intunsigned` int(20) UNSIGNED,
  `bigintunsigned` bigint(100) UNSIGNED,
  `tinyint` tinyint(5),
  `smallint` smallint(10),
  `mediumint` mediumint(15),
  `int` int(20),
  `bigint` bigint(100),
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

-- 行过滤表
CREATE TABLE `drc4`.`row_filter` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '空',
  `uid` char(30) DEFAULT NULL COMMENT '空',
  `charlt256` char(30) DEFAULT NULL COMMENT '空',
  `chareq256` char(128) DEFAULT NULL COMMENT '空',
  `chargt256` char(255) DEFAULT NULL COMMENT '空',
  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '空',
  `varchareq256` varchar(256) DEFAULT NULL COMMENT '空',
  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '空',
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  `drc_id_int` int(11) NOT NULL DEFAULT '1' COMMENT '空',
  `addcol1` varchar(64) DEFAULT 'default_addcol1' COMMENT 'test',
  `addcol2` varchar(64) DEFAULT 'default_addcol2' COMMENT 'test',
  `drc_char_test_2` char(30) DEFAULT 'char' COMMENT '空',
  `drc_tinyint_test_2` tinyint(4) DEFAULT '12' COMMENT '空',
  `drc_bigint_test` bigint(20) DEFAULT '120' COMMENT '空',
  `drc_integer_test` int(11) DEFAULT '11' COMMENT '空',
  `drc_mediumint_test` mediumint(9) DEFAULT '12345' COMMENT '空',
  `drc_time6_test` time DEFAULT '02:02:02' COMMENT '空',
  `drc_datetime3_test` datetime(3) DEFAULT '2019-01-01 01:01:01.000' COMMENT '空',
  `drc_year_test` year(4) DEFAULT '2020' COMMENT '空',
  `hourly_rate_3` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT '空',
  `drc_numeric10_4_test` decimal(10,4) DEFAULT '100.0000' COMMENT '空',
  `drc_float_test` float DEFAULT '12' COMMENT '空',
  `drc_double_test` double DEFAULT '123' COMMENT '空',
  `drc_bit4_test` bit(4) DEFAULT b'11' COMMENT 'TEST',
  `drc_double10_4_test` double(10,4) DEFAULT '123.1245' COMMENT '空',
  `drc_real_test` double DEFAULT '234' COMMENT '空',
  `drc_real10_4_test` double(10,4) DEFAULT '23.4000' COMMENT '空',
  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002' COMMENT '空',
  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '空',
  `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',
  PRIMARY KEY (`id`),
  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';

-- 延时监控表
CREATE DATABASE IF NOT EXISTS drcmonitordb;
CREATE TABLE IF NOT EXISTS `drcmonitordb`.`delaymonitor` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `src_ip` varchar(15) NOT NULL,
  `dest_ip` varchar(256) NOT NULL,
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY(`id`)
) ENGINE=InnoDB;

-- 事务表阻断循环复制
CREATE DATABASE IF NOT EXISTS drcmonitordb;
CREATE TABLE IF NOT EXISTS `drcmonitordb`.`gtid_executed` (
  `id` int(11) NOT NULL,
  `server_uuid` char(36) NOT NULL,
  `gno` bigint(20) NOT NULL,
  `gtidset` longtext,
  PRIMARY KEY ix_gtid(`id`,`server_uuid`)
);

-- 压测
CREATE DATABASE IF NOT EXISTS bbzbbzdrcbenchmarktmpdb;
CREATE TABLE `bbzbbzdrcbenchmarktmpdb`.`benchmark` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '空',
  `charlt256` char(30) DEFAULT NULL COMMENT '空',
  `chareq256` char(128) DEFAULT NULL COMMENT '空',
  `chargt256` char(255) DEFAULT NULL COMMENT '空',
  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '空',
  `varchareq256` varchar(256) DEFAULT NULL COMMENT '空',
  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '空',
  `tinyint1` tinyint(4) DEFAULT NULL COMMENT '空',
  `bigint1` bigint(20) DEFAULT NULL COMMENT '空',
  `integer1` int(11) DEFAULT NULL COMMENT '空',
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB COMMENT='压测';

CREATE TABLE `bbzbbzdrcbenchmarktmpdb`.`benchmark1` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '空',
  `charlt256` char(30) DEFAULT NULL COMMENT '空',
  `chareq256` char(128) DEFAULT NULL COMMENT '空',
  `chargt256` char(255) DEFAULT NULL COMMENT '空',
  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '空',
  `varchareq256` varchar(256) DEFAULT NULL COMMENT '空',
  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '空',
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  `drc_id_int` int(11) NOT NULL DEFAULT '1' COMMENT '空',
  `addcol1` varchar(64) DEFAULT 'default_addcol1' COMMENT 'test',
  `addcol2` varchar(64) DEFAULT 'default_addcol2' COMMENT 'test',
  `drc_char_test_2` char(30) DEFAULT 'char' COMMENT '空',
  `drc_tinyint_test_2` tinyint(4) DEFAULT '12' COMMENT '空',
  `drc_bigint_test` bigint(20) DEFAULT '120' COMMENT '空',
  `drc_integer_test` int(11) DEFAULT '11' COMMENT '空',
  `drc_mediumint_test` mediumint(9) DEFAULT '12345' COMMENT '空',
  `drc_time6_test` time DEFAULT '02:02:02' COMMENT '空',
  `drc_datetime3_test` datetime(3) DEFAULT '2019-01-01 01:01:01.000' COMMENT '空',
  `drc_year_test` year(4) DEFAULT '2020' COMMENT '空',
  `hourly_rate_3` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT '空',
  `drc_numeric10_4_test` decimal(10,4) DEFAULT '100.0000' COMMENT '空',
  `drc_float_test` float DEFAULT '12' COMMENT '空',
  `drc_double_test` double DEFAULT '123' COMMENT '空',
  `drc_bit4_test` bit(4) DEFAULT b'11' COMMENT 'TEST',
  `drc_double10_4_test` double(10,4) DEFAULT '123.1245' COMMENT '空',
  `drc_real_test` double DEFAULT '234' COMMENT '空',
  `drc_real10_4_test` double(10,4) DEFAULT '23.4000' COMMENT '空',
  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0' COMMENT '空',
  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '空',
  `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',
  PRIMARY KEY (`id`),
  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';

CREATE TABLE `bbzbbzdrcbenchmarktmpdb`.`benchmark2` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '空',
  `charlt256` char(30) DEFAULT NULL COMMENT '空',
  `chareq256` char(128) DEFAULT NULL COMMENT '空',
  `chargt256` char(255) DEFAULT NULL COMMENT '空',
  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '空',
  `varchareq256` varchar(256) DEFAULT NULL COMMENT '空',
  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '空',
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  `drc_id_int` int(11) NOT NULL DEFAULT '1' COMMENT '空',
  `addcol1` varchar(64) DEFAULT 'default_addcol1' COMMENT 'test',
  `addcol2` varchar(64) DEFAULT 'default_addcol2' COMMENT 'test',
  `drc_char_test_2` char(30) DEFAULT 'char' COMMENT '空',
  `drc_tinyint_test_2` tinyint(4) DEFAULT '12' COMMENT '空',
  `drc_bigint_test` bigint(20) DEFAULT '120' COMMENT '空',
  `drc_integer_test` int(11) DEFAULT '11' COMMENT '空',
  `drc_mediumint_test` mediumint(9) DEFAULT '12345' COMMENT '空',
  `drc_time6_test` time DEFAULT '02:02:02' COMMENT '空',
  `drc_datetime3_test` datetime(3) DEFAULT '2019-01-01 01:01:01.000' COMMENT '空',
  `drc_year_test` year(4) DEFAULT '2020' COMMENT '空',
  `hourly_rate_3` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT '空',
  `drc_numeric10_4_test` decimal(10,4) DEFAULT '100.0000' COMMENT '空',
  `drc_float_test` float DEFAULT '12' COMMENT '空',
  `drc_double_test` double DEFAULT '123' COMMENT '空',
  `drc_bit4_test` bit(1) DEFAULT b'1' COMMENT 'TEST',
  `drc_double10_4_test` double(10,4) DEFAULT '123.1245' COMMENT '空',
  `drc_real_test` double DEFAULT '234' COMMENT '空',
  `drc_real10_4_test` double(10,4) DEFAULT '23.4000' COMMENT '空',
  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0' COMMENT '空',
  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '空',
  `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',
  PRIMARY KEY (`id`),
  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';

CREATE TABLE `drc4`.`grand_transaction` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '空',
  `charlt256` char(30) DEFAULT NULL COMMENT '空',
  `chareq256` char(128) DEFAULT NULL COMMENT '空',
  `chargt256` char(255) DEFAULT NULL COMMENT '空',
  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '空',
  `varchareq256` varchar(256) DEFAULT NULL COMMENT '空',
  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '空',
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  `drc_id_int` int(11) NOT NULL DEFAULT '1' COMMENT '空',
  `addcol1` varchar(64) DEFAULT 'default_addcol1' COMMENT 'test',
  `addcol2` varchar(64) DEFAULT 'default_addcol2' COMMENT 'test',
  `drc_char_test_2` char(30) DEFAULT 'char' COMMENT '空',
  `drc_tinyint_test_2` tinyint(4) DEFAULT '12' COMMENT '空',
  `drc_bigint_test` bigint(20) DEFAULT '120' COMMENT '空',
  `drc_integer_test` int(11) DEFAULT '11' COMMENT '空',
  `drc_mediumint_test` mediumint(9) DEFAULT '12345' COMMENT '空',
  `drc_time6_test` time DEFAULT '02:02:02' COMMENT '空',
  `drc_datetime3_test` datetime(3) DEFAULT '2019-01-01 01:01:01.000' COMMENT '空',
  `drc_year_test` year(4) DEFAULT '2020' COMMENT '空',
  `hourly_rate_3` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT '空',
  `drc_numeric10_4_test` decimal(10,4) DEFAULT '100.0000' COMMENT '空',
  `drc_float_test` float DEFAULT '12' COMMENT '空',
  `drc_double_test` double DEFAULT '123' COMMENT '空',
  `drc_bit4_test` bit(1) DEFAULT b'1' COMMENT 'TEST',
  `drc_double10_4_test` double(10,4) DEFAULT '123.1245' COMMENT '空',
  `drc_real_test` double DEFAULT '234' COMMENT '空',
  `drc_real10_4_test` double(10,4) DEFAULT '23.4000' COMMENT '空',
  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0' COMMENT '空',
  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '空',
  `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',
  PRIMARY KEY (`id`),
  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='function_grand_transaction_test';

CREATE TABLE `bbzbbzdrcbenchmarktmpdb`.`benchmark3` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '空',
  `charlt256` char(30) DEFAULT NULL COMMENT '空',
  `chareq256` char(128) DEFAULT NULL COMMENT '空',
  `chargt256` char(255) DEFAULT NULL COMMENT '空',
  `varcharlt256` varchar(30) DEFAULT NULL COMMENT '空',
  `varchareq256` varchar(256) DEFAULT NULL COMMENT '空',
  `varchargt256` varchar(12000) CHARACTER SET utf8 DEFAULT NULL COMMENT '空',
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  `drc_id_int` int(11) NOT NULL DEFAULT '1' COMMENT '空',
  `addcol1` varchar(64) DEFAULT 'default_addcol1' COMMENT 'test',
  `addcol2` varchar(64) DEFAULT 'default_addcol2' COMMENT 'test',
  `drc_char_test_2` char(30) DEFAULT 'char' COMMENT '空',
  `drc_tinyint_test_2` tinyint(4) DEFAULT '12' COMMENT '空',
  `drc_bigint_test` bigint(20) DEFAULT '120' COMMENT '空',
  `drc_integer_test` int(11) DEFAULT '11' COMMENT '空',
  `drc_mediumint_test` mediumint(9) DEFAULT '12345' COMMENT '空',
  `drc_time6_test` time DEFAULT '02:02:02' COMMENT '空',
  `drc_datetime3_test` datetime(3) DEFAULT '2019-01-01 01:01:01.000' COMMENT '空',
  `drc_year_test` year(4) DEFAULT '2020' COMMENT '空',
  `hourly_rate_3` decimal(10,2) NOT NULL DEFAULT '1.00' COMMENT '空',
  `drc_numeric10_4_test` decimal(10,4) DEFAULT '100.0000' COMMENT '空',
  `drc_float_test` float DEFAULT '12' COMMENT '空',
  `drc_double_test` double DEFAULT '123' COMMENT '空',
  `drc_bit4_test` bit(1) DEFAULT b'1' COMMENT 'TEST',
  `drc_double10_4_test` double(10,4) DEFAULT '123.1245' COMMENT '空',
  `drc_real_test` double DEFAULT '234' COMMENT '空',
  `drc_real10_4_test` double(10,4) DEFAULT '23.4000' COMMENT '空',
  `drc_binary200_test_2` binary(200) DEFAULT 'binary2002\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0' COMMENT '空',
  `drc_varbinary1800_test_2` varbinary(1800) DEFAULT 'varbinary1800' COMMENT '空',
  `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name',
  PRIMARY KEY (`id`),
  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='test';

CREATE TABLE `bbzbbzdrcbenchmarktmpdb`.`drc_dal_test` (
id int NOT NULL AUTO_INCREMENT COMMENT '空' ,
charlt256 char(30)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL default null COMMENT '空' ,
chareq256 char(128)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL default null COMMENT '空' ,
chargt256 char(255)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL default null COMMENT '空' ,
varcharlt256 varchar(30)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL default null COMMENT '空' ,
varchareq256 varchar(256)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL default null COMMENT '空' ,
varchargt256 varchar(12000)  CHARACTER SET utf8 COLLATE utf8_general_ci NULL default null COMMENT '空' ,
datachange_lasttime timestamp(3)  NOT NULL default CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间' ,
drc_id_int int NOT NULL default '1' COMMENT '空' ,
addcol1 varchar(64)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL default 'default_addcol1' COMMENT 'test' ,
addcol2 varchar(64)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL default 'default_addcol2' COMMENT 'test' ,
drc_char_test_2 char(30)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL default 'char' COMMENT '空' ,
drc_tinyint_test_2 tinyint NULL default '12' COMMENT '空' ,
drc_bigint_test bigint NULL default '120' COMMENT '空' ,
drc_integer_test int NULL default '11' COMMENT '空' ,
drc_mediumint_test mediumint NULL default '12345' COMMENT '空' ,
drc_time6_test time NULL default '02:02:02' COMMENT '空' ,
drc_datetime3_test datetime(3)  NULL default '2019-01-01 01:01:01.000' COMMENT '空' ,
drc_year_test year NULL default '2020' COMMENT '空' ,
hourly_rate_3 decimal(10,2)  NOT NULL default '1.00' COMMENT '空' ,
drc_numeric10_4_test decimal(10,4)  NULL default '100.0000' COMMENT '空' ,
drc_float_test float(12)  NULL default '12' COMMENT '空' ,
drc_double_test double NULL default '123' COMMENT '空' ,
drc_bit4_test bit NULL default b'1' COMMENT 'TEST' ,
drc_double10_4_test double(10,4)  NULL default '123.1245' COMMENT '空' ,
drc_real_test double NULL default '234' COMMENT '空' ,
drc_real10_4_test double(10,4)  NULL default '23.4000' COMMENT '空' ,
drc_binary200_test_2 binary(200)  NULL default 'binary2002' COMMENT '空' ,
drc_varbinary1800_test_2 varbinary(1800)  NULL default 'varbinary1800' COMMENT '空' ,
addcol varchar(50)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL default 'addColName' COMMENT '添加普通Name' ,PRIMARY KEY (id),KEY ix_DataChange_LastTime (datachange_lasttime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='test';
