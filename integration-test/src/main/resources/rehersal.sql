drop database if exists bbzdrcbenchmarkdb;

create database if not exists bbzdrcbenchmarkdb character set utf8;

CREATE TABLE `bbzdrcbenchmarkdb`.`benchmark1` (
  `id` int(11) not null AUTO_INCREMENT,
  `charlt256` char(30) CHARACTER SET gbk,
  `chareq256` char(128) CHARACTER SET cp932,
  `chargt256` char(255) CHARACTER SET euckr,
  `varcharlt256` varchar(30) CHARACTER SET utf8mb4,
  `varchareq256` varchar(256) CHARACTER SET latin1,
  `varchargt256` varchar(12000) CHARACTER SET utf8,
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

alter table `bbzdrcbenchmarkdb`.`benchmark1` ADD COLUMN addcol VARCHAR(55) DEFAULT 'addCol' COMMENT '添加普通' after datachange_lasttime;

insert into `bbzdrcbenchmarkdb`.`benchmark1` (`charlt256`, `chareq256`, `varcharlt256`, `varchargt256`, `datachange_lasttime`) values('中国1', '愚かな1', 'присоска1', 'abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ababc11abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abbcbc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ac1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1c1abc1abc1abc1abc1abc1abc1abc1abc1abc1a', NOW());

alter table `bbzdrcbenchmarkdb`.`benchmark1` modify `addcol` varchar(50) DEFAULT 'addColName' COMMENT '添加普通Name'

insert into `bbzdrcbenchmarkdb`.`benchmark1` (`charlt256`, `chareq256`, `varcharlt256`, `varchargt256`, `datachange_lasttime`) values('中国1', '愚かな1', 'присоска1', 'abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ababc11abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abbcbc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ac1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1c1abc1abc1abc1abc1abc1abc1abc1abc1abc1a', NOW());

create unique index ind_charlt256 ON `bbzdrcbenchmarkdb`.`benchmark1`(charlt256, id);

insert into `bbzdrcbenchmarkdb`.`benchmark1` (`charlt256`, `chareq256`, `varcharlt256`, `varchargt256`, `datachange_lasttime`) values('中国1', '愚かな1', 'присоска1', 'abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ababc11abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abbcbc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ac1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1c1abc1abc1abc1abc1abc1abc1abc1abc1abc1a', NOW());

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_id_int int not null default 1 after datachange_lasttime

insert into `bbzdrcbenchmarkdb`.`benchmark1` (`charlt256`, `chareq256`, `varcharlt256`, `varchargt256`, `datachange_lasttime`) values('中国1', '愚かな1', 'присоска1', 'abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ababc11abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abbcbc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ac1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1c1abc1abc1abc1abc1abc1abc1abc1abc1abc1a', NOW());

alter table `bbzdrcbenchmarkdb`.`benchmark1` ADD COLUMN addcol1 VARCHAR(64) DEFAULT 'default_addcol1' COMMENT 'test' after drc_id_int, ADD COLUMN addcol2 VARCHAR(64) DEFAULT 'default_addcol2' COMMENT 'test' after addcol1

insert into `bbzdrcbenchmarkdb`.`benchmark1` (`charlt256`, `chareq256`, `varcharlt256`, `varchargt256`, `datachange_lasttime`) values('中国1', '愚かな1', 'присоска1', 'abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ababc11abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abbcbc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1ac1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1abc1c1abc1abc1abc1abc1abc1abc1abc1abc1abc1a', NOW());

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_char_test_2 char(30) default 'char' after addcol2

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_tinyint_test_2 tinyint(5) default 12 after drc_char_test_2

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_bigint_test bigint(100) default 120 after drc_tinyint_test_2

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_integer_test integer(50) default 11 after drc_bigint_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_mediumint_test  mediumint(15) default 12345 after drc_integer_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_time6_test time(6) default '02:02:02' after drc_mediumint_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_datetime3_test datetime(3) default '2019-01-01 01:01:01' after drc_time6_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_year_test year(4) default '2020' after drc_datetime3_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` ADD COLUMN hourly_rate_3 decimal(10,2) NOT NULL DEFAULT 1.00  after drc_year_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_numeric10_4_test numeric(10,4) default 100 after hourly_rate_3

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_float_test float default 12 after drc_numeric10_4_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_double_test double default 123 after drc_float_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_bit4_test bit(4) default b'11' after drc_double_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_double10_4_test double(10,4) default 123.1245 after drc_double_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_real_test real default 234 after drc_double10_4_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_real10_4_test real(10,4) default 23.4 after drc_real_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_binary200_test_2 binary(200) default 'binary2002'  after drc_real10_4_test

alter table `bbzdrcbenchmarkdb`.`benchmark1` Add COLUMN drc_varbinary1800_test_2 varbinary(1800) default 'varbinary1800'  after drc_binary200_test_2

drop table if exists `bbzdrcbenchmarkdb`.`benchmark1`;

drop database if exists bbzdrcbenchmarkdb;