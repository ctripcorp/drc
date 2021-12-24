create database drc1;

CREATE TABLE `drc1`.`t` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `one` varchar(30) DEFAULT "one",
                        `two` varchar(1000) DEFAULT "two",
                        `three` char(30),
                        `four` char(255),
                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
                        PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;