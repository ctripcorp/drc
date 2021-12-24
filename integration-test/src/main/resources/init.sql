-- 场景1：事务多个事件跨库
create database drc1;

create database drc2;

create database drc3;

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

update `drc4`.`update1` set `four`="fourwruof" where `id`=xxx;


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

insert into `drc4`.`component` (`charlt256`, `chareq256`, `varcharlt256`, `varchareq256`, `datachange_lasttime`) values
('中国1', '愚かな1', 'присоска1', 'abc1', '2019-10-16 15:15:15.666661'),
('中国2', '愚かな2', 'присоска2', 'abc2', '2019-10-16 15:15:15.666662'),
('中国3', '愚かな3', 'присоска3', 'abc3', '2019-10-16 15:15:15.666663');

-- 场景4：binlog_row_image=minimal,full,noblob
set binlog_row_image = 'full';
insert into `drc4`.`component` (`charlt256`, `chareq256`, `varcharlt256`, `varchareq256`, `datachange_lasttime`) values
('中国1', '愚かな1', 'присоска1', 'abc1', '2019-10-16 15:15:15.666664'),
('中国2', '愚かな2', 'присоска2', 'abc2', '2019-10-16 15:15:15.666665'),
('中国3', '愚かな3', 'присоска3', 'abc3', '2019-10-16 15:15:15.666666');

set binlog_row_image = 'minimal';
insert into `drc4`.`component` (`charlt256`, `chareq256`, `varcharlt256`, `varchareq256`, `datachange_lasttime`) values
('中国1', '愚かな1', 'присоска1', 'abc1', '2019-10-16 15:15:15.666667'),
('中国2', '愚かな2', 'присоска2', 'abc2', '2019-10-16 15:15:15.666668'),
('中国3', '愚かな3', 'присоска3', 'abc3', '2019-10-16 15:15:15.666669');

set binlog_row_image = 'noblob';
insert into `drc4`.`component` (`charlt256`, `chareq256`, `varcharlt256`, `varchareq256`, `datachange_lasttime`) values
('中国1', '愚かな1', 'присоска1', 'abc1', '2019-10-16 15:15:15.666610'),
('中国2', '愚かな2', 'присоска2', 'abc2', '2019-10-16 15:15:15.666611'),
('中国3', '愚かな3', 'присоска3', 'abc3', '2019-10-16 15:15:15.666612');

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

-- 最大极端取值
insert into `drc4`.`multi_type_number` values (255, 65535, 16777215, 4294967295, 1099511627775, 281474976710655, 72057594037927935, 18446744073709551615,
  127, 32767, 8388607, 2147483647, 2147483647, 9223372036854775807, '2019-10-16 15:15:15.666613'
);
-- 最小极端取值
insert into `drc4`.`multi_type_number` values (0, 0, 0, 0, 0, 0, 0, 0,
  -128, -32768, -8388608, -2147483648, -2147483648, -9223372036854775808, '2019-10-16 15:15:15.666614'
);

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

-- 最大极端取值
insert into `drc4`.`multi_type_number_unsigned` values (0, 0, 0, 0, 0, 0, 72057594037927935, 18446744073709551615,
  255, 65535, 16777215, 4294967295, 4294967295, 18446744073709551615, '2019-10-16 15:15:15.666615'
);

-- 取值介于[signed max, unsigned max]之间
insert into `drc4`.`multi_type_number_unsigned` values (200, 40000, 10000000, 3000000000, 600000000000, 200000000000000, 40000000000000000, 10000000000000000000,
  200, 40000, 10000000, 3000000000, 3000000000, 10000000000000000000, '2019-10-16 15:15:15.666616'
);


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
insert into `drc4`.`float_type` values (
0, -123456789.123456789, 123456.123456789,
-123456789.123456789, 123456.123456789,
-123456789.123456789, 123456.123456789,
11111111112222222222333333333344444444445555555555666666666677777,
-11111111112222222222333333333344444444445555555555666666666677777,
0.111111111122222222223333333333,
-0.111111111122222222223333333333,
11111111112222222222333333333344444.111111111122222222223333333333,
-11111111112222222222333333333344444.111111111122222222223333333333,
99999999999999999999999999999999999999999999999999999999999999999,
0.000000000000000000000000000001,
-0.000000000000000000000000000001,
-99999999999999999999999999999999999999999999999999999999999999999,
-123456789.123456789, 123456.123456789, '2019-10-16 15:15:15.666617'
);

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

-- insert blob type, load_file("/xx/xx.png") or x'hex string'
-- binary存储二进制，varbinary存储字符
insert into `drc4`.`charset_type` values (
0,'嘿嘿嘿varchar4000', '嘿嘿嘿char1000', 'varbinary1800',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
'嘿嘿嘿tinytext', '嘿嘿嘿mediumtext', '嘿嘿嘿text', '嘿嘿嘿longtext',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
'2019-10-16 15:15:15.666618'
);

-- varbinary存储二进制，binary存储字符
insert into `drc4`.`charset_type` values (
0,'嘿嘿嘿varchar4000', '嘿嘿嘿char1000',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
'binary200',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
'嘿嘿嘿tinytext', '嘿嘿嘿mediumtext', '嘿嘿嘿text', '嘿嘿嘿longtext',
x'8b5fa55d1301000000530000004618000000003e040000000001000464726334000c636861727365745f74797065000c0ffe0ffefcfcfcfcfcfcfcfc10a00fcee80807fec80103020401030204ff0f12f93547',
'2019-10-16 15:15:15.666619'
);


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

-- 随机值测试
insert into `drc4`.`time_type` values (
0,'2019-10-16 15:15:15.666666',
'15:15:15.666666',
'15:15:15.666666',
'2019-10-16 15:15:15.666666',
'2019-10-16 15:15:15.666666',
'2019-10-16 15:15:15.666666',
'2019-10-16 15:15:15.666666',
'2019',
'19',
 12344,
 '2019-10-16 15:15:15.666620'
);

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
insert into `drc4`.`time_type_boundary` values (
'0000-01-01 00:00:00.000000',
'9999-12-31 23:59:59.999999',

'-838:59:59.000000',
'838:59:59.000000',
'-11:11:11.000000',
'00:00:00.000000',
'11:11:11.000000',
'-838:59:59.000000',
'838:59:59.000000',

'0000-01-01 00:00:00.000000',
'9999-12-31 23:59:59.999999',
'2019-10-17 11:59:59.999999',
'2019-10-17 23:59:59.999999',
'2019-10-17 01:59:59.999999',
'0000-01-01 00:00:00.000000',
'9999-12-31 23:59:59.999999',

'1970-01-01 08:00:01.000000',
'2038-01-19 11:14:07.999999',
'2038-01-19 11:14:07.999999',
'2038-01-19 11:14:07.999999',
'2038-01-19 11:14:07.999999',
'1970-01-01 08:00:01.000000',
'2038-01-19 11:14:07.999999',

'0',
'2115'
);

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

-- 延时监控表
CREATE DATABASE IF NOT EXISTS drcmonitordb;
CREATE TABLE IF NOT EXISTS `drcmonitordb`.`delaymonitor` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `src_ip` varchar(15) NOT NULL,
  `dest_ip` varchar(15) NOT NULL,
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY(`id`)
) ENGINE=InnoDB;
INSERT INTO `drcmonitordb`.`delaymonitor`(`src_ip`, `dest_ip`) VALUES('test_oy', 'test_oy');
UPDATE `drcmonitordb`.`delaymonitor` SET `datachange_lasttime`=CURRENT_TIMESTAMP(3) WHERE `dest_ip`='test_oy';

-- 压测
CREATE DATABASE IF NOT EXISTS bbzdrcbenchmarkdb;
CREATE TABLE `bbzdrcbenchmarkdb`.`benchmark` (
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
