drop table if exists `ghost1_unitest`.`t1`;

create table if not exists `ghost1_unitest`.`t1` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(20),
  `myname` varchar(20) DEFAULT 'myname',
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  primary key(`id`,`name`)
)engine = innoDB;

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`) values ('value1', '2019-10-16 15:15:15.666661');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`) values ('value2', '2019-10-16 15:15:15.666662');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`) values ('value3', '2019-10-16 15:15:15.666663');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value1';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value2';

delete from `ghost1_unitest`.`t1` where `name` = 'value1';

delete from `ghost1_unitest`.`t1` where `name` = 'value3';

ADD COLUMN `addcol` VARCHAR(55) DEFAULT 'addcol' COMMENT '添加普通' after `datachange_lasttime`;

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value4', '2019-10-16 15:15:15.666661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value5', '2019-10-16 15:15:15.666662', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value6', '2019-10-16 15:15:15.666663', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value4';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value5';

delete from `ghost1_unitest`.`t1` where `name` = 'value4';

delete from `ghost1_unitest`.`t1` where `name` = 'value5';

modify name varchar(50);

insert into `ghost1_unitest`.`t1` ( `name`, `datachange_lasttime`, `addcol`) values ('value7', '2019-10-16 15:15:15.666661', 'addcol');

insert into `ghost1_unitest`.`t1` ( `name`, `datachange_lasttime`, `addcol`) values ('value8', '2019-10-16 15:15:15.666662', 'addcol');

insert into `ghost1_unitest`.`t1` ( `name`, `datachange_lasttime`, `addcol`) values ('value9', '2019-10-16 15:15:15.666663', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value7';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value8';

delete from `ghost1_unitest`.`t1` where `name` = 'value7';

delete from `ghost1_unitest`.`t1` where `name` = 'value9';

modify name varchar(15) DEFAULT 'name';

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value10', '2019-10-16 15:15:15.642661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value11', '2019-10-16 15:15:15.642662', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value12', '2019-10-16 15:15:15.642663', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value10';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value11';

delete from `ghost1_unitest`.`t1` where `name` = 'value10';

delete from `ghost1_unitest`.`t1` where `name` = 'value12';

drop addcol;

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`) values ('value13', '2019-10-16 15:15:15.634661');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`) values ('value14', '2019-10-16 15:15:15.634662');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`) values ('value15', '2019-10-16 15:15:15.634663');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value13';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value14';

delete from `ghost1_unitest`.`t1` where `name` = 'value13';

delete from `ghost1_unitest`.`t1` where `name` = 'value14';

delete from `ghost1_unitest`.`t1`;

ADD COLUMN addcol VARCHAR(55) DEFAULT 'addcol' COMMENT '添加普通' after datachange_lasttime;

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value1', '2019-10-16 15:15:15.566661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value2', '2019-10-16 15:15:15.566661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value3', '2019-10-16 15:15:15.566661', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value1';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value2';

delete from `ghost1_unitest`.`t1` where `name` = 'value1';

delete from `ghost1_unitest`.`t1` where `name` = 'value2';

ADD unique index ind_col(addcol, id)

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value4', '2019-10-16 15:15:15.623661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value5', '2019-10-16 15:15:15.623661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value6', '2019-10-16 15:15:15.623661', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value4';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value5';

delete from `ghost1_unitest`.`t1` where `name` = 'value4';

delete from `ghost1_unitest`.`t1` where `name` = 'value6';

drop index ind_col

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value7', '2019-10-16 15:15:15.666661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value8', '2019-10-16 15:15:15.666661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value9', '2019-10-16 15:15:15.666661', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value7';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value8';

delete from `ghost1_unitest`.`t1` where `name` = 'value7';

delete from `ghost1_unitest`.`t1` where `name` = 'value9';

Add COLUMN drc_id int not null default 1 after addcol

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu11', '2005-10-06 12:09:49.762', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu12', '2005-10-06 12:09:49.762', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu13', '2005-10-06 12:09:49.762', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu11';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu12';

delete from `ghost1_unitest`.`t1` where `name` = 'valu11';

delete from `ghost1_unitest`.`t1` where `name` = 'valu13';

ADD COLUMN addcol1 VARCHAR(64) DEFAULT 'default_addcol1' COMMENT 'test' after drc_id, ADD COLUMN addcol2 VARCHAR(64) DEFAULT 'default_addcol2' COMMENT 'test' after addcol1

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu21', '2025-10-06 10:39:19.962', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu22', '2025-10-06 10:39:19.962', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu23', '2025-10-06 10:39:19.962', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu21';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu22';

delete from `ghost1_unitest`.`t1` where `name` = 'valu21';

delete from `ghost1_unitest`.`t1` where `name` = 'valu23';

Add COLUMN drc_id_test int(11) NOT NULL default 123

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu31', '2026-10-06 10:39:19.962', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu32', '2026-10-06 10:39:19.962', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu33', '2026-10-06 10:39:19.962', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu31';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu32';

delete from `ghost1_unitest`.`t1` where `name` = 'valu31';

delete from `ghost1_unitest`.`t1` where `name` = 'valu32';

Add COLUMN drc_char_test char(30) default 'char'

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu41', '2027-10-06 10:39:19.963', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu42', '2027-10-06 10:39:19.963', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu43', '2027-10-06 10:39:19.963', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu41';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu42';

delete from `ghost1_unitest`.`t1` where `name` = 'valu41';

delete from `ghost1_unitest`.`t1` where `name` = 'valu42';

Add COLUMN drc_tinyint_test tinyint(5) default 12

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu51', '2028-10-06 10:39:19.964', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu52', '2028-10-06 10:39:19.964', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu53', '2028-10-06 10:39:19.964', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu51';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu52';

delete from `ghost1_unitest`.`t1` where `name` = 'valu51';

delete from `ghost1_unitest`.`t1` where `name` = 'valu52';

Add COLUMN drc_bigint_test bigint(100) default 120

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu61', '2029-10-06 10:39:19.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu62', '2029-10-06 10:39:19.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu63', '2029-10-06 10:39:19.965', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu61';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu62';

delete from `ghost1_unitest`.`t1` where `name` = 'valu61';

delete from `ghost1_unitest`.`t1` where `name` = 'valu62';

Add COLUMN drc_integer_test integer(50) default 11

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu71', '2030-10-06 10:39:19.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu72', '2030-10-06 10:39:19.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu73', '2030-10-06 10:39:19.965', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu71';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu72';

delete from `ghost1_unitest`.`t1` where `name` = 'valu71';

delete from `ghost1_unitest`.`t1` where `name` = 'valu73';

Add COLUMN drc_mediumint_test  mediumint(15) default 12345

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu81', '2031-10-06 10:39:19.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu82', '2031-10-06 10:39:19.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu83', '2031-10-06 10:39:19.965', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu81';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu82';

delete from `ghost1_unitest`.`t1` where `name` = 'valu81';

delete from `ghost1_unitest`.`t1` where `name` = 'valu83';

Add COLUMN drc_time6_test time(6) default '02:02:02'

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu901', '2031-10-06 10:34:19.930', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu902', '2031-10-06 10:34:19.930', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu903', '2031-10-06 10:34:19.930', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu901';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu902';

delete from `ghost1_unitest`.`t1` where `name` = 'valu901';

delete from `ghost1_unitest`.`t1` where `name` = 'valu902';

Add COLUMN drc_datetime3_test datetime(3) default '2019-01-01 01:01:01'

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu911', '2031-10-06 10:34:19.931', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu912', '2031-10-06 10:34:19.931', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu913', '2031-10-06 10:34:19.931', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu911';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu912';

delete from `ghost1_unitest`.`t1` where `name` = 'valu911';

delete from `ghost1_unitest`.`t1` where `name` = 'valu912';

Add COLUMN drc_year_test year(4) default '2020'

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu891', '2031-10-06 10:34:19.929', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu892', '2031-10-06 10:34:19.929', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu893', '2031-10-06 10:34:19.929', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu891';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu892';

delete from `ghost1_unitest`.`t1` where `name` = 'valu891';

delete from `ghost1_unitest`.`t1` where `name` = 'valu892';

Add COLUMN drc_binary200_test binary(200) default 'binary200'

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu861', '2031-10-06 10:34:19.926', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu862', '2031-10-06 10:34:19.926', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu863', '2031-10-06 10:34:19.926', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu861';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu862';

delete from `ghost1_unitest`.`t1` where `name` = 'valu861';

delete from `ghost1_unitest`.`t1` where `name` = 'valu862';

Add COLUMN drc_varbinary1800_test varbinary(1800) default 'varbinary1800'

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu851', '2031-10-06 10:34:19.925', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu852', '2031-10-06 10:34:19.925', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu853', '2031-10-06 10:34:19.925', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu851';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu852';

delete from `ghost1_unitest`.`t1` where `name` = 'valu851';

delete from `ghost1_unitest`.`t1` where `name` = 'valu853';

Add COLUMN drc_numeric10_4_test numeric(10,4) default 100

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu841', '2031-10-06 10:34:19.924', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu842', '2031-10-06 10:34:19.924', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu843', '2031-10-06 10:34:19.924', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu841';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu842';

delete from `ghost1_unitest`.`t1` where `name` = 'valu841';

delete from `ghost1_unitest`.`t1` where `name` = 'valu842';

Add COLUMN drc_float_test float default 12

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu831', '2031-10-06 10:39:19.924', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu832', '2031-10-06 10:39:19.924', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu833', '2031-10-06 10:39:19.924', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu831';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu832';

delete from `ghost1_unitest`.`t1` where `name` = 'valu831';

delete from `ghost1_unitest`.`t1` where `name` = 'valu832';

Add COLUMN drc_double_test double default 123

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu821', '2031-10-06 10:39:19.964', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu822', '2031-10-06 10:39:19.964', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu823', '2031-10-06 10:39:19.964', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu821';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu822';

delete from `ghost1_unitest`.`t1` where `name` = 'valu821';

delete from `ghost1_unitest`.`t1` where `name` = 'valu823';

Add COLUMN drc_double10_4_test double(10,4) default 123.1245

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu811', '2031-10-06 10:39:19.964', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu812', '2031-10-06 10:39:19.964', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu813', '2031-10-06 10:39:19.964', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu811';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu811';

delete from `ghost1_unitest`.`t1` where `name` = 'valu811';

delete from `ghost1_unitest`.`t1` where `name` = 'valu811';

ADD COLUMN hourly_rate decimal(10,2) NOT NULL DEFAULT 1.00

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value1', '2015-10-16 15:05:19.661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value2', '2015-10-16 15:05:19.661', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('value3', '2015-10-16 15:05:19.661', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value1';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'value2';

delete from `ghost1_unitest`.`t1` where `name` = 'value1';

delete from `ghost1_unitest`.`t1` where `name` = 'value3';

Add COLUMN drc_bit4_test bit(4) default b'0111'

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu71', '2031-10-06 10:39:19.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu72', '2031-10-06 10:39:19.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu73', '2031-10-06 10:39:19.965', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu71';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu71';

delete from `ghost1_unitest`.`t1` where `name` = 'valu71';

delete from `ghost1_unitest`.`t1` where `name` = 'valu73';

Add COLUMN drc_real_test real default 234

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu91', '2031-10-06 10:39:18.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu92', '2031-10-06 10:39:18.965', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu93', '2031-10-06 10:39:18.965', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu91';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu92';

delete from `ghost1_unitest`.`t1` where `name` = 'valu91';

delete from `ghost1_unitest`.`t1` where `name` = 'valu92';

Add COLUMN drc_real10_4_test real(10,4) default 23.4

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu101', '2031-10-06 10:39:19.923', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu102', '2031-10-06 10:39:19.923', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu103', '2031-10-06 10:39:19.923', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu101';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu102';

delete from `ghost1_unitest`.`t1` where `name` = 'valu101';

delete from `ghost1_unitest`.`t1` where `name` = 'valu103';

DROP PRIMARY KEY, ADD PRIMARY KEY (`id`)

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu111', '2031-10-06 10:39:19.924', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu112', '2031-10-06 10:39:19.924', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('valu113', '2031-10-06 10:39:19.924', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu111';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'valu112';

delete from `ghost1_unitest`.`t1` where `name` = 'valu111';

delete from `ghost1_unitest`.`t1` where `name` = 'valu112';

change myname myname2 varchar(15) default 'myname2'

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('name21', '2025-10-06 10:39:19.962', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('name22', '2025-10-06 10:39:19.962', 'addcol');

insert into `ghost1_unitest`.`t1` (`name`, `datachange_lasttime`, `addcol`) values ('name23', '2025-10-06 10:39:19.962', 'addcol');

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'name21';

update `ghost1_unitest`.`t1` set `datachange_lasttime` = now() where `name` = 'name22';

delete from `ghost1_unitest`.`t1` where `name` = 'name21';

delete from `ghost1_unitest`.`t1` where `name` = 'name23';
